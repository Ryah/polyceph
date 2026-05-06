import { MODULE_NAME, VERSION, generationMutexEvents } from './js/constants.js';
import { loadSettings, getAvailableProfiles, refreshPresets, settings, getActivePipeline } from './js/state.js';
import { initUI, updateChatSelectorOptions, updateSendButtonVisibility } from './js/ui/ui.js';
import { addSettingsUI } from './js/ui/settings-ui.js';
import { startPipeline, runPipeline, rerunPipelineFromStep, clearOrphanedIndicators } from './js/engine.js';
import { injectChatPipelineSelector } from './js/ui/chat-ui.js';
import { postMessageToChat, ensureChatSaved } from './js/compat-shared.js';
import { getSlashCommandParserModule, getSlashCommandModule, getSlashCommandArgumentModule, getScriptModule, getOpenAIModule } from './js/compat-st.js';
import { isEditorOpen, saveActiveEdit } from './js/ui/ui-shared.js';
import { logger } from './js/logger.js';

// -------------------------------------------------------------------------
// Interception Hook
// -------------------------------------------------------------------------

/**
 * Dedicated handler for the custom Polyceph send button.
 */
export async function handlePolycephSend(e) {
    if (settings.activePipelineId === 'none') return;
    logger.debug('Polyceph send button clicked.');
    if (e) {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();
    }
    return await processSendAction();
}

/**
 * Interceptor for SillyTavern's native send actions (Enter key, send button).
 */
export async function interceptSend(e) {
    if (settings.activePipelineId === 'none') return;

    // Check Enter key behavior settings
    if (e.type === 'keydown') {
        const behavior = settings.enterBehavior || 'all';
        if (behavior === 'none') return;

        const isMobile = window.matchMedia("(pointer: coarse)").matches;
        if (behavior === 'pc' && isMobile) return;
        if (behavior === 'mobile' && !isMobile) return;

        // Standard Enter checks
        if (e.key !== 'Enter' || e.shiftKey) return;
    }

    const textarea = document.getElementById('send_textarea');
    if (!textarea) return;

    let text = textarea.value.trim();
    if (await tryHandlePolycephSlashCommand(textarea, text)) {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();
        return;
    }

    if (text.startsWith('/')) {
        logger.debug('Slash command detected, bypassing Polyceph intercept.');
        return;
    }

    e.preventDefault();
    e.stopPropagation();
    e.stopImmediatePropagation();

    return await processSendAction();
}

let lastSendTime = 0;
let slashCommandsRegistered = false;

/**
 * Core logic for processing a send action.
 */
async function processSendAction() {
    const now = Date.now();
    if (now - lastSendTime < 500) {
        logger.warn(`Duplicate send action blocked by debouncing.`);
        return;
    }
    lastSendTime = now;

    const textarea = document.getElementById('send_textarea');
    const text = textarea ? textarea.value.trim() : '';

    if (await tryHandlePolycephSlashCommand(textarea, text)) {
        return;
    }

    // Handle active message edit if present
    if (isEditorOpen()) {
        logger.info('Active message edit detected. Saving before pipeline run.');
        try {
            const saved = await saveActiveEdit();
            if (!saved) {
                logger.error('Editor failed to close after save attempt.');
                toastr.error('Message editor failed to close. Pipeline aborted to prevent data loss.', 'Polyceph');
                return;
            }
            // Give ST a tiny bit more time to settle the chat array update
            await new Promise(r => setTimeout(r, 100));
        } catch (err) {
            logger.error('Error during editor save:', err);
            toastr.error('Error saving message edit. Pipeline aborted.', 'Polyceph');
            return;
        }
    }

    const context = SillyTavern.getContext();

    if (typeof context.deactivateSendButtons === 'function') {
        context.deactivateSendButtons();
    } else if (typeof window.is_send_press !== 'undefined') {
        window.is_send_press = true;
    }

    if (!text) {
        const lastMsg = context.chat[context.chat.length - 1];
        if (lastMsg && lastMsg.is_user && !lastMsg.extra?.polyceph_typing) {
            logger.info('Empty input detected. Re-triggering pipeline.');
            if (!lastMsg.extra) lastMsg.extra = {};
            lastMsg.extra.polyceph_typing = true;
            lastMsg.extra.polyceph_active_tasks = [];

            if (typeof context.updateMessageBlock === 'function') {
                context.updateMessageBlock(context.chat.length - 1, lastMsg);
            }
            try {
                await startPipeline(lastMsg.mes);
            } catch (err) {
                logger.error('Pipeline re-trigger failed:', err);
            }
        }
        return;
    }

    if (textarea) textarea.value = '';

    const userName = context.name1 || 'User';
    const avatarStr = typeof context.getThumbnailUrl === 'function' && context.userAvatar ?
        context.getThumbnailUrl('avatar', context.userAvatar) : '';

    if (settings.emulateCoreEvents && typeof SillyTavern !== 'undefined' && generationMutexEvents) {
        await context.eventSource.emit(generationMutexEvents.MUTEX_CAPTURED, { extension_name: MODULE_NAME });
    }

    postMessageToChat({
        content: text,
        name: userName,
        isUser: true,
        is_system: false,
        send_date: typeof context.humanizedDateTime === 'function' ? context.humanizedDateTime() : new Date().toLocaleString(),
        mes: text,
        forceAvatar: avatarStr,
        extra: { polyceph_typing: true, polyceph_active_tasks: [] },
        swipes: [text],
        swipe_id: 0,
        swipe_info: [{}]
    });

    await ensureChatSaved();

    try {
        await startPipeline(text);
    } catch (err) {
        logger.error('Pipeline execution failed:', err);
        toastr.error('Pipeline execution failed.', 'Polyceph');
    }
}

function findLatestPolycephMessageId() {
    const context = SillyTavern.getContext();
    for (let i = context.chat.length - 1; i >= 0; i--) {
        const msg = context.chat[i];
        if (msg?.extra?.polyceph_source === 'polyceph' && msg?.extra?.polyceph_batch) {
            return i;
        }
    }
    return -1;
}

function parseRerunArgs(rawArgs) {
    const text = Array.isArray(rawArgs)
        ? rawArgs.map(v => String(v ?? '')).join(' ').trim()
        : String(rawArgs ?? '').trim();

    if (!text) return { error: 'Usage: /polyceph-rerun <step> [mesId]' };

    const parts = text.split(/\s+/).filter(Boolean);
    const fromStep = Number(parts[0]);

    if (!Number.isInteger(fromStep) || fromStep < 1) {
        return { error: 'Usage: /polyceph-rerun <step> [mesId]' };
    }

    let targetMesId = Number(parts[1]);
    if (!Number.isInteger(targetMesId) || targetMesId < 0) {
        targetMesId = findLatestPolycephMessageId();
    }

    return { fromStep, targetMesId };
}

async function executeRerunFromStep(fromStep, targetMesId, textarea = null) {
    const activePipeline = getActivePipeline();
    const totalSteps = activePipeline?.steps?.length || 0;
    if (totalSteps > 0 && fromStep > totalSteps) {
        toastr.warning(`Step ${fromStep} is out of range. Pipeline has ${totalSteps} steps.`, 'Polyceph');
        return true;
    }

    if (targetMesId < 0) {
        toastr.warning('No Polyceph message found. Provide a mesId or run a pipeline first.', 'Polyceph');
        return true;
    }

    const context = SillyTavern.getContext();
    const targetMsg = context.chat[targetMesId];
    const batchId = targetMsg?.extra?.polyceph_batch;

    if (!batchId) {
        toastr.warning(`Message ${targetMesId} is not a Polyceph output.`, 'Polyceph');
        return true;
    }

    let triggeringUserMesId = -1;
    for (let i = targetMesId - 1; i >= 0; i--) {
        if (context.chat[i]?.is_user) {
            triggeringUserMesId = i;
            break;
        }
    }

    if (textarea) textarea.value = '';
    toastr.info(`Re-running Polyceph from step ${fromStep}...`, 'Polyceph');
    await rerunPipelineFromStep(batchId, fromStep, triggeringUserMesId);
    return true;
}

async function tryHandlePolycephSlashCommand(textarea, text) {
    if (!text || !text.startsWith('/')) return false;

    const match = text.match(/^\/(?:polyceph-rerun|poly-rerun|prerun)\s+(.+)$/i);
    if (!match) return false;

    const parsed = parseRerunArgs(match[1]);
    if (parsed.error) {
        toastr.warning(parsed.error, 'Polyceph');
        return true;
    }

    return await executeRerunFromStep(parsed.fromStep, parsed.targetMesId, textarea);
}

async function registerPolycephSlashCommands() {
    if (slashCommandsRegistered) return;

    try {
        const parserMod = await getSlashCommandParserModule();
        const slashMod = await getSlashCommandModule();
        const argMod = await getSlashCommandArgumentModule();

        const SlashCommandParser = parserMod?.SlashCommandParser;
        const SlashCommand = slashMod?.SlashCommand;
        const SlashCommandArgument = argMod?.SlashCommandArgument;
        const ARGUMENT_TYPE = argMod?.ARGUMENT_TYPE;

        if (!SlashCommandParser || !SlashCommand || !SlashCommandArgument || !ARGUMENT_TYPE) {
            logger.warn('[Polyceph] Slash command modules unavailable; global slash commands not registered.');
            return;
        }

        if (SlashCommandParser.commands?.['polyceph-rerun']) {
            slashCommandsRegistered = true;
            return;
        }

        SlashCommandParser.addCommandObject(SlashCommand.fromProps({
            name: 'polyceph-rerun',
            aliases: ['poly-rerun', 'prerun'],
            returns: ARGUMENT_TYPE.STRING,
            helpString: 'Rerun a Polyceph batch from a step. Usage: /polyceph-rerun <step> [mesId]',
            unnamedArgumentList: [
                SlashCommandArgument.fromProps({
                    description: 'Step and optional message id. Example: "5" or "5 123"',
                    typeList: [ARGUMENT_TYPE.STRING],
                    isRequired: true,
                }),
            ],
            callback: async (_args, value) => {
                const parsed = parseRerunArgs(value);
                if (parsed.error) {
                    toastr.warning(parsed.error, 'Polyceph');
                    return '';
                }

                await executeRerunFromStep(parsed.fromStep, parsed.targetMesId, null);
                return '';
            },
        }));

        slashCommandsRegistered = true;
        logger.info('[Polyceph] Registered slash commands: /polyceph-rerun, /poly-rerun, /prerun');
    } catch (err) {
        logger.warn('[Polyceph] Failed to register slash commands:', err);
    }
}

function interceptSwipe(e) {
    if (settings.activePipelineId === 'none') return;
    const swipeRightBtn = e.target.closest('.swipe_right');
    if (!swipeRightBtn) return;

    const mesBlock = e.target.closest('.mes');
    if (!mesBlock) return;

    const mesId = parseInt(mesBlock.getAttribute('mesid'));
    const context = SillyTavern.getContext();
    const chatMsg = context.chat[mesId];

    if (!chatMsg || !chatMsg.extra || chatMsg.extra.polyceph_source !== 'polyceph') return;

    const swipeId = chatMsg.swipe_id || 0;
    const swipesLen = chatMsg.swipes ? chatMsg.swipes.length : 1;

    if (swipeId === swipesLen - 1) {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();

        const batchId = chatMsg.extra.polyceph_batch;
        const userInput = chatMsg.extra.polyceph_input || '';

        let triggeringUserMesId = -1;
        for (let i = mesId - 1; i >= 0; i--) {
            if (context.chat[i]?.is_user) {
                triggeringUserMesId = i;
                break;
            }
        }

        toastr.info('Polyceph generation running...', 'Polyceph');
        runPipeline(userInput, batchId, triggeringUserMesId);
    }
}

function setupIntercepts() {
    const rightForm = document.getElementById('rightSendForm');
    const textArea = document.getElementById('send_textarea');

    if (rightForm) {
        const handleSendEvent = (e) => {
            if (e.target.closest('#polyceph-send-button') || e.target.closest('#polyceph-stop-button') || e.target.closest('#polyceph-chat-pipeline-container')) {
                return;
            }
            const sendBtn = e.target.closest('#send_but');
            if (sendBtn && settings.interceptSend !== false) {
                interceptSend(e);
            }
        };
        rightForm.addEventListener('click', handleSendEvent, true);
        rightForm.addEventListener('mousedown', handleSendEvent, true);

        const observer = new MutationObserver(() => {
            if (!document.getElementById('polyceph-chat-pipeline-container')) {
                injectChatPipelineSelector(handlePolycephSend);
            }
        });
        observer.observe(rightForm, { childList: true, subtree: true });
    }

    if (textArea) textArea.addEventListener('keydown', (e) => {
        if (settings.enterBehavior !== 'none') {
            interceptSend(e);
        }
    }, true);
    document.body.addEventListener('click', interceptSwipe, true);
}

// -------------------------------------------------------------------------
// Polyceph Continue Button
// -------------------------------------------------------------------------

let _generateFn = null;

async function getGenerateFn() {
    if (_generateFn) return _generateFn;
    const mod = await getScriptModule();
    _generateFn = mod?.Generate || null;
    return _generateFn;
}

function setupPolycephContinueButton() {
    // Inject button into message template so all messages get it
    const extraButtons = document.querySelector('#message_template .mes_buttons .extraMesButtons');
    if (!extraButtons) {
        logger.warn('[Polyceph] Could not find extraMesButtons in message template.');
        return;
    }

    if (extraButtons.querySelector('.polyceph-continue-button')) return; // already injected

    const btn = document.createElement('div');
    btn.className = 'mes_button polyceph-continue-button fa-solid fa-forward interactable';
    btn.title = 'Continue with Polyceph reasoning';
    btn.setAttribute('tabindex', '0');
    btn.style.display = 'none'; // hidden by default; shown only on polyceph messages
    extraButtons.prepend(btn);

    // Show/hide button per message on render
    const context = SillyTavern.getContext();
    if (context.eventSource && context.eventTypes) {
        const onMessageRendered = (mesId) => {
            const mesEl = document.querySelector(`#chat [mesid="${mesId}"]`);
            if (!mesEl) return;
            const msg = context.chat[mesId];
            const isPolyceph = !!(msg?.extra?.polyceph_batch);
            const btnEl = mesEl.querySelector('.polyceph-continue-button');
            if (btnEl) btnEl.style.display = isPolyceph ? 'flex' : 'none';
        };
        context.eventSource.on(context.eventTypes.CHARACTER_MESSAGE_RENDERED, onMessageRendered);
    }

    // Click delegation for the continue button
    $(document).on('click', '.polyceph-continue-button', async function (e) {
        e.preventDefault();
        e.stopPropagation();

        const mesEl = $(this).closest('.mes');
        if (!mesEl.length) return;

        const mesId = parseInt(mesEl.attr('mesid'));
        const ctx = SillyTavern.getContext();
        const msg = ctx.chat[mesId];
        if (!msg?.extra?.polyceph_batch) {
            toastr.warning('This message does not have Polyceph reasoning context.', 'Polyceph');
            return;
        }

        // Build context string from the polyceph context vault snapshot
        const contextVault = msg.extra.polyceph_context || {};
        const contextLines = Object.entries(contextVault)
            .filter(([, v]) => v && typeof v === 'string' && v.trim())
            .map(([k, v]) => `[${k}: ${v}]`);

        const stepContextStr = contextLines.length > 0
            ? `\n\nPrevious Polyceph reasoning step outputs:\n${contextLines.join('\n')}`
            : '';

        const Generate = await getGenerateFn();
        if (!Generate) {
            toastr.error('Could not load SillyTavern Generate function.', 'Polyceph');
            return;
        }

        // Use GENERATE_BEFORE_COMBINE_PROMPTS to inject the context once.
        // For text-completion APIs, patch the jailbreak slot.
        // For chat-completion APIs, temporarily patch oai_settings.continue_nudge_prompt.
        if (stepContextStr && ctx.eventSource && ctx.eventTypes) {
            // Text-completion injection via GENERATE_BEFORE_COMBINE_PROMPTS
            const injectOnce = (data) => {
                if (typeof data.jailbreak === 'string') {
                    data.jailbreak = (data.jailbreak || '') + stepContextStr;
                } else if (typeof data.main === 'string') {
                    data.main = (data.main || '') + stepContextStr;
                }
            };
            ctx.eventSource.once(ctx.eventTypes.GENERATE_BEFORE_COMBINE_PROMPTS, injectOnce);

            // Chat-completion injection: temporarily patch oai_settings.continue_nudge_prompt
            try {
                const oaiMod = await getOpenAIModule();
                const oaiSettings = oaiMod?.oai_settings;
                if (oaiSettings && typeof oaiSettings.continue_nudge_prompt === 'string') {
                    const originalNudge = oaiSettings.continue_nudge_prompt;
                    oaiSettings.continue_nudge_prompt = originalNudge + stepContextStr;
                    const restoreNudge = () => { oaiSettings.continue_nudge_prompt = originalNudge; };
                    ctx.eventSource.once(ctx.eventTypes.GENERATION_ENDED, restoreNudge);
                    ctx.eventSource.once(ctx.eventTypes.GENERATION_STOPPED, restoreNudge);
                }
            } catch (_) { /* openai.js unavailable, skip chat-completion nudge patch */ }
        }

        await Generate('continue');
    });
}

// -------------------------------------------------------------------------
// Init
// -------------------------------------------------------------------------

async function init() {
    logger.info(`Initializing Polyceph v${VERSION}...`);

    await loadSettings();
    await clearOrphanedIndicators();
    await getAvailableProfiles();
    refreshPresets();

    // Initialize UI Subsystems
    initUI();
    addSettingsUI();
    await registerPolycephSlashCommands();
    setupIntercepts();
    injectChatPipelineSelector(handlePolycephSend);
    setupPolycephContinueButton();

    const context = SillyTavern.getContext();
    if (context.eventSource && context.eventTypes) {
        // Sync chat buttons on settings/state changes
        context.eventSource.on(context.eventTypes.SETTINGS_UPDATED, () => {
            updateChatSelectorOptions();
            updateSendButtonVisibility();
        });
        context.eventSource.on('polyceph-settings-changed', updateSendButtonVisibility);

        context.eventSource.on('polyceph-pipeline-started', () => {
            document.body.classList.add('polyceph-pipeline-active');
            updateSendButtonVisibility();
        });
        context.eventSource.on('polyceph-pipeline-ended', () => {
            document.body.classList.remove('polyceph-pipeline-active');
            updateSendButtonVisibility();
        });
    }

    logger.info('Polyceph loaded.');
}

if (typeof jQuery !== 'undefined') {
    jQuery(async () => { await init(); });
} else {
    window.addEventListener('DOMContentLoaded', init);
}
