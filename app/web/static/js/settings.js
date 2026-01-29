/**
 * JavaScript для страницы настроек (упрощённая версия)
 */

$(document).ready(function() {
    loadAllSettings();
});

// Уведомления
function showToast(msg, type = 'success') {
    const id = 'toast-' + Date.now();
    const cls = type === 'success' ? 'bg-success' : type === 'error' ? 'bg-danger' : 'bg-warning';
    $(`<div id="${id}" class="toast ${cls} text-white"><div class="toast-header ${cls} text-white"><strong>${type === 'success' ? '✅' : '❌'}</strong></div><div class="toast-body">${msg}</div></div>`)
        .appendTo('#toast-container');
    const t = new bootstrap.Toast(document.getElementById(id));
    t.show();
    $(`#${id}`).on('hidden.bs.toast', function() { $(this).remove(); });
}

// Загрузка всех настроек
function loadAllSettings() {
    loadGeneralSettings();
    loadApiSettings();
    loadDailyDigest();
    loadWeeklyDigest();
    loadMonthlyDigest();
    loadIntervals();
    loadPrompts();
    loadParsersStatus();
}

// Основные настройки
function loadGeneralSettings() {
    $.get('/api/settings/config', function(data) {
        if (data.success && data.config) {
            const c = data.config.app;
            $('#bypassing_method').val(c.bypassing_method || 'LOOP');
            $('#model_type').val(c.model_type || 'LLM_API');
            $('#interest_threshold').val(c.interest_threshold || 0.2);
            $('#topic').val(c.topic || '');
            $('#max_news_per_channel').val(c.max_news_per_channel || 5);
            $('#max_news_per_digest').val(c.max_news_per_digest || 7);
            $('#max_news_time_period').val(c.max_news_time_period || 86400);
        }
    }).fail(function(xhr) { showToast(xhr.responseJSON?.message || 'Ошибка', 'error'); });
}

function saveGeneralSettings(e) {
    e.preventDefault();
    saveSettings('/api/settings/save', { section: 'app', config_updates: {
        BYPASSING_METHOD: $('#bypassing_method').val(),
        MODEL_TYPE: $('#model_type').val(),
        INTEREST_THRESHOLD: parseFloat($('#interest_threshold').val()),
        TOPIC: $('#topic').val(),
        MAX_NEWS_PER_CHANNEL: parseInt($('#max_news_per_channel').val()),
        MAX_NEWS_PER_DIGEST: parseInt($('#max_news_per_digest').val()),
        MAX_NEWS_TIME_PERIOD: parseInt($('#max_news_time_period').val())
    }}, 'Основные настройки сохранены', loadGeneralSettings);
}

// API настройки
function loadApiSettings() {
    $.get('/api/settings/api', function(data) {
        if (data.success && data.api) {
            const a = data.api;
            $('#google_spreadsheet_id').val(a.google_spreadsheet_id || '');
            $('#telegram_bot_token').val(a.telegram_bot_token || '');
            $('#telegram_channel_id').val(a.telegram_channel_id || '');
            $('#telegram_api_id').val(a.telegram_api_id || '');
            $('#telegram_api_hash').val(a.telegram_api_hash || '');
            $('#telegram_2fa_password').val(a.telegram_2fa_password || '');
            $('#telegram_phone').val(a.telegram_phone || '');
            $('#twitter_bearer_token').val(a.twitter_bearer_token || '');
            $('#youtube_api_key').val(a.youtube_api_key || '');
            $('#groq_api_key').val(a.groq_api_key || '');
        }
    }).fail(function(xhr) { showToast(xhr.responseJSON?.message || 'Ошибка', 'error'); });
}

function saveApiSettings(e) {
    e.preventDefault();
    saveSettings('/api/settings/save', { section: 'api', config_updates: {
        TELEGRAM_BOT_TOKEN: $('#telegram_bot_token').val(),
        TELEGRAM_CHANNEL_ID: $('#telegram_channel_id').val(),
        TELEGRAM_API_ID: $('#telegram_api_id').val(),
        TELEGRAM_API_HASH: $('#telegram_api_hash').val(),
        TELEGRAM_2FA_PASSWORD: $('#telegram_2fa_password').val(),
        TELEGRAM_PHONE: $('#telegram_phone').val(),
        TWITTER_BEARER_TOKEN: $('#twitter_bearer_token').val(),
        YOUTUBE_API_KEY: $('#youtube_api_key').val(),
        GROQ_API_KEY: $('#groq_api_key').val(),
        GOOGLE_SPREADSHEET_ID: $('#google_spreadsheet_id').val()
    }}, 'API настройки сохранены', loadApiSettings);
}

// Ежедневный дайджест
function loadDailyDigest() {
    $.get('/api/settings/config', function(data) {
        if (data.success && data.config) {
            const d = data.config.digest_schedule.daily;
            if (d) {
                $('#daily_hour').val(String(d.hour).padStart(2,'0') + ':' + String(d.minute).padStart(2,'0'));
                $('#daily_enabled').prop('checked', d.enabled !== false);
            }
        }
    }).fail(function(xhr) { showToast(xhr.responseJSON?.message || 'Ошибка', 'error'); });
}

function saveDailyDigest(e) {
    e.preventDefault();
    saveSettings('/api/settings/save', { section: 'scheduler', config_updates: {
        DAILY_DIGEST: JSON.stringify({
            hour: parseInt($('#daily_hour').val().split(':')[0]),
            minute: parseInt($('#daily_hour').val().split(':')[1]),
            enabled: $('#daily_enabled').is(':checked')
        })
    }}, 'Ежедневный дайджест сохранён', loadDailyDigest);
}

// Еженедельный дайджест
function loadWeeklyDigest() {
    $.get('/api/settings/config', function(data) {
        if (data.success && data.config) {
            const w = data.config.digest_schedule.weekly;
            if (w) {
                const days = ['mon','tue','wed','thu','fri','sat','sun'];
                $('#weekly_day').val(days.indexOf(w.day_of_week) || 6);
                $('#weekly_hour').val(String(w.hour).padStart(2,'0') + ':' + String(w.minute).padStart(2,'0'));
                $('#weekly_enabled').prop('checked', w.enabled !== false);
            }
        }
    }).fail(function(xhr) { showToast(xhr.responseJSON?.message || 'Ошибка', 'error'); });
}

function saveWeeklyDigest(e) {
    e.preventDefault();
    const days = ['mon','tue','wed','thu','fri','sat','sun'];
    saveSettings('/api/settings/save', { section: 'scheduler', config_updates: {
        WEEKLY_DIGEST: JSON.stringify({
            day_of_week: days[parseInt($('#weekly_day').val())],
            hour: parseInt($('#weekly_hour').val().split(':')[0]),
            minute: parseInt($('#weekly_hour').val().split(':')[1]),
            enabled: $('#weekly_enabled').is(':checked')
        })
    }}, 'Еженедельный дайджест сохранён', loadWeeklyDigest);
}

// Ежемесячный дайджест
function loadMonthlyDigest() {
    $.get('/api/settings/config', function(data) {
        if (data.success && data.config) {
            const m = data.config.digest_schedule.monthly;
            if (m) {
                $('#monthly_day').val(m.day || 28);
                $('#monthly_hour').val(String(m.hour).padStart(2,'0') + ':' + String(m.minute).padStart(2,'0'));
                $('#monthly_enabled').prop('checked', m.enabled !== false);
            }
        }
    }).fail(function(xhr) { showToast(xhr.responseJSON?.message || 'Ошибка', 'error'); });
}

function saveMonthlyDigest(e) {
    e.preventDefault();
    saveSettings('/api/settings/save', { section: 'scheduler', config_updates: {
        MONTHLY_DIGEST: JSON.stringify({
            day: parseInt($('#monthly_day').val()),
            hour: parseInt($('#monthly_hour').val().split(':')[0]),
            minute: parseInt($('#monthly_hour').val().split(':')[1]),
            enabled: $('#monthly_enabled').is(':checked')
        })
    }}, 'Ежемесячный дайджест сохранён', loadMonthlyDigest);
}

// Интервалы сбора
function loadIntervals() {
    $.get('/api/settings/config', function(data) {
        if (data.success && data.config) {
            const i = data.config.intervals;
            $('#interval_twitter').val(i.twitter || 900);
            $('#interval_telegram').val(i.telegram || 5);
            $('#interval_youtube').val(i.youtube || 5);
        }
    }).fail(function(xhr) { showToast(xhr.responseJSON?.message || 'Ошибка', 'error'); });
}

function saveIntervals(e) {
    e.preventDefault();
    saveSettings('/api/settings/save', { section: 'parsers', config_updates: {
        TWITTER_INTERVAL: parseInt($('#interval_twitter').val()),
        TELEGRAM_INTERVAL: parseInt($('#interval_telegram').val()),
        YOUTUBE_INTERVAL: parseInt($('#interval_youtube').val())
    }}, 'Интервалы сохранены', loadIntervals);
}

// Промпты
function loadPrompts() {
    $.get('/api/settings/prompts', function(data) {
        if (data.success && data.prompts) {
            $('#ad_detection_prompt').val(data.prompts.AD_DETECTION_PROMPT || '');
            $('#interest_scoring_prompt').val(data.prompts.INTEREST_SCORING_PROMPT || '');
            $('#digest_processing_prompt').val(data.prompts.DIGEST_PROCESSING_PROMPT || '');
        }
    }).fail(function(xhr) { showToast(xhr.responseJSON?.message || 'Ошибка', 'error'); });
}

function savePrompts(e) {
    e.preventDefault();
    saveSettings('/api/settings/save', { prompts_updates: {
        AD_DETECTION_PROMPT: $('#ad_detection_prompt').val(),
        INTEREST_SCORING_PROMPT: $('#interest_scoring_prompt').val(),
        DIGEST_PROCESSING_PROMPT: $('#digest_processing_prompt').val()
    }}, 'Промпты сохранены', loadPrompts);
}

// Статус парсеров
function loadParsersStatus() {
    $.get('/api/settings/config', function(data) {
        if (data.success && data.config) {
            const p = data.config.parser_status;
            $('#twitter_enabled').prop('checked', p.twitter || false);
            $('#telegram_enabled').prop('checked', p.telegram || false);
            $('#youtube_enabled').prop('checked', p.youtube || false);
        }
    }).fail(function(xhr) { showToast(xhr.responseJSON?.message || 'Ошибка', 'error'); });
}

function saveParsersStatus(e) {
    e.preventDefault();
    saveSettings('/api/settings/save', { section: 'parsers', config_updates: {
        TWITTER_ACTIVE: $('#twitter_enabled').is(':checked') ? 'TRUE' : 'FALSE',
        TELEGRAM_ACTIVE: $('#telegram_enabled').is(':checked') ? 'TRUE' : 'FALSE',
        YOUTUBE_ACTIVE: $('#youtube_enabled').is(':checked') ? 'TRUE' : 'FALSE'
    }}, 'Статус парсеров сохранён', loadParsersStatus);
}

// Унифицированная функция сохранения
function saveSettings(url, data, successMsg, reloadFn) {
    $.ajax({
        url: url, method: 'POST', contentType: 'application/json',
        data: JSON.stringify(data),
        success: function(r) {
            showToast(successMsg);
            if (reloadFn) reloadFn();
        },
        error: function(xhr) {
            showToast(xhr.responseJSON?.message || 'Ошибка сохранения', 'error');
        }
    });
}
