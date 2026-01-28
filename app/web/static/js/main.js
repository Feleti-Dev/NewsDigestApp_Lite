/**
 * Основной JavaScript файл для веб-интерфейса
 */

// Глобальные утилиты
window.NewsDigestApp = {
    // Конфигурация
    config: {
        apiBase: '/api',
        refreshInterval: 30000, // 30 секунд
        version: '1.0.0'
    },

    // Состояние
    state: {
        lastUpdate: null,
        isLoading: false,
        errors: []
    },

    // Утилиты
    utils: {
        formatDate: function(dateString) {
            if (!dateString) return 'N/A';
            const date = new Date(dateString);
            return date.toLocaleDateString('ru-RU') + ' ' + date.toLocaleTimeString('ru-RU');
        },

        formatNumber: function(num) {
            if (num === null || num === undefined) return '0';
            return num.toLocaleString('ru-RU');
        },

        truncateText: function(text, maxLength) {
            if (!text) return '';
            if (text.length <= maxLength) return text;
            return text.substring(0, maxLength - 3) + '...';
        },

        getStatusBadge: function(status) {
            const badges = {
                'connected': '<span class="badge bg-success">Подключен</span>',
                'disconnected': '<span class="badge bg-danger">Отключен</span>',
                'error': '<span class="badge bg-danger">Ошибка</span>',
                'warning': '<span class="badge bg-warning">Предупреждение</span>',
                'running': '<span class="badge bg-success">Запущен</span>',
                'stopped': '<span class="badge bg-secondary">Остановлен</span>',
                'unknown': '<span class="badge bg-secondary">Неизвестно</span>'
            };
            return badges[status] || badges['unknown'];
        },

        showToast: function(message, type = 'info') {
            // Создаем toast уведомление
            const toastId = 'toast-' + Date.now();
            const toastHtml = `
                <div id="${toastId}" class="toast align-items-center text-bg-${type} border-0" role="alert">
                    <div class="d-flex">
                        <div class="toast-body">
                            ${message}
                        </div>
                        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
                    </div>
                </div>
            `;

            // Добавляем в контейнер
            let container = document.getElementById('toast-container');
            if (!container) {
                // Создаем контейнер если его нет
                const newContainer = document.createElement('div');
                newContainer.id = 'toast-container';
                newContainer.className = 'toast-container position-fixed bottom-0 end-0 p-3';
                newContainer.style.zIndex = '1055';
                document.body.appendChild(newContainer);
                container = newContainer;
            }

            container.innerHTML += toastHtml;

            // Показываем toast
            const toastElement = document.getElementById(toastId);
            const toast = new bootstrap.Toast(toastElement, { delay: 3000 });
            toast.show();

            // Удаляем после скрытия
            toastElement.addEventListener('hidden.bs.toast', function () {
                toastElement.remove();
            });
        }
    },

    // API методы
    api: {
        get: function(endpoint, callback) {
            $.get(this.config.apiBase + endpoint, function(data) {
                if (data.error) {
                    NewsDigestApp.utils.showToast('Ошибка: ' + data.error, 'danger');
                    console.error('API Error:', data.error);
                }
                callback(data);
            }).fail(function(xhr, status, error) {
                NewsDigestApp.utils.showToast('Ошибка соединения с API', 'danger');
                console.error('API Request Failed:', status, error);
            });
        },

        post: function(endpoint, data, callback) {
            $.post(this.config.apiBase + endpoint, JSON.stringify(data), function(response) {
                if (response.error) {
                    NewsDigestApp.utils.showToast('Ошибка: ' + response.error, 'danger');
                } else if (response.message) {
                    NewsDigestApp.utils.showToast(response.message, 'success');
                }
                callback(response);
            }, 'json').fail(function(xhr, status, error) {
                NewsDigestApp.utils.showToast('Ошибка соединения с API', 'danger');
                console.error('API Request Failed:', status, error);
            });
        }
    }
};

// Инициализация при загрузке страницы
$(document).ready(function() {
    console.log('News Digest App Web Interface v' + NewsDigestApp.config.version);

    // Инициализация всех компонентов, требующих JavaScript
    initTooltips();
    initAutoRefresh();

    // Проверка соединения
    checkConnection();
});

function initTooltips() {
    // Включаем Bootstrap tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
}

function initAutoRefresh() {
    // Автоматическое обновление данных на странице
    const refreshElements = document.querySelectorAll('[data-refresh]');
    refreshElements.forEach(element => {
        const interval = parseInt(element.getAttribute('data-refresh-interval')) ||
                         NewsDigestApp.config.refreshInterval;

        setInterval(() => {
            // Перезагрузка данных для элемента
            // Реализация зависит от конкретного элемента
        }, interval);
    });
}

function checkConnection() {
    // Проверка соединения с API
    $.get('/api/system/status', function(data) {
        if (data.database && data.database.connected) {
            console.log('System is online');
        }
    }).fail(function() {
        NewsDigestApp.utils.showToast('Не удалось подключиться к серверу', 'warning');
    });
}

// Глобальные обработчики ошибок
window.addEventListener('error', function(event) {
    console.error('Global error:', event.error);
    NewsDigestApp.state.errors.push(event.error);
});

// Хелперы для работы с формами
function disableForm(formId) {
    const form = document.getElementById(formId);
    if (form) {
        const elements = form.elements;
        for (let i = 0; i < elements.length; i++) {
            elements[i].disabled = true;
        }
    }
}

function enableForm(formId) {
    const form = document.getElementById(formId);
    if (form) {
        const elements = form.elements;
        for (let i = 0; i < elements.length; i++) {
            elements[i].disabled = false;
        }
    }
}

// Экспорт данных
function exportToJson(data, filename) {
    const dataStr = JSON.stringify(data, null, 2);
    const dataUri = 'data:application/json;charset=utf-8,'+ encodeURIComponent(dataStr);

    const exportFileDefaultName = filename || 'export.json';

    const linkElement = document.createElement('a');
    linkElement.setAttribute('href', dataUri);
    linkElement.setAttribute('download', exportFileDefaultName);
    linkElement.click();
}

// Копирование в буфер обмена
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        NewsDigestApp.utils.showToast('Скопировано в буфер обмена', 'success');
    }).catch(err => {
        console.error('Failed to copy:', err);
        NewsDigestApp.utils.showToast('Ошибка копирования', 'danger');
    });
}

function controlTask(taskType, action, digestType = null) {
    const data = {
        task_type: taskType,
        action: action
    };

    if (digestType) {
        data.digest_type = digestType;
    }

    $.ajax({
        url: '/api/tasks/control',
        type: 'POST',
        contentType: 'application/json',
        data: JSON.stringify(data),
        success: function(response) {
            console.log(response.success, response.message)
            if (response.success) {
                NewsDigestApp.utils.showToast(response.message, 'success');
                // Обновляем статус через 2 секунды
//                setTimeout(loadSystemStatus, 2000);
//                setTimeout(loadTasksStatus, 2000);
            } else {
//                NewsDigestApp.utils.showError(response.message || 'Ошибка выполнения');
            }
        },
        error: function(xhr, status, error) {
            let message = 'Ошибка соединения с сервером';
            if (xhr.responseJSON && xhr.responseJSON.message) {
                message = xhr.responseJSON.message;
            }
            console.log(message)
//            NewsDigestApp.utils.showError(message);
        }
    });
}