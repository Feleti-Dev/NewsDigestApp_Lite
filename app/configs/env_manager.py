"""
Менеджер для работы с .env файлом.
Позволяет сохранять конфигурацию с сохранением комментариев и структуры.
"""
import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)


class EnvManager:
    """
    Менеджер для работы с .env файлом.

    Функционал:
    - Чтение конфигурации с сохранением структуры
    - Обновление значений с сохранением комментариев
    - Форматирование различных типов данных (bool, dict, list)
    """

    def __init__(self, env_path: str = ".env"):
        """
        Инициализация менеджера.

        Args:
            env_path: путь к .env файлу (относительно корня проекта)
        """
        self.env_path = env_path
        self._structure = None

    def _load_structure(self) -> Dict[int, Dict[str, Any]]:
        """
        Загрузка структуры .env файла.

        Returns:
            Словарь: {номер_строки: {content, type, key}}
        """
        if not os.path.exists(self.env_path):
            logger.warning(f"Файл .env не найден: {self.env_path}")
            return {}

        structure = {}

        with open(self.env_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                stripped = line.strip()

                line_info = {
                    'original': line,
                    'content': stripped,
                    'type': 'empty'
                }

                if not stripped:
                    line_info['type'] = 'empty'
                elif stripped.startswith('#'):
                    line_info['type'] = 'comment'
                elif '=' in stripped:
                    parts = stripped.split('=', 1)
                    key = parts[0].strip()
                    line_info['type'] = 'key_value'
                    line_info['key'] = key
                    line_info['value'] = parts[1].strip() if len(parts) > 1 else ''

                structure[i] = line_info

        self._structure = structure
        return structure

    def get(self, key: str, default: Any = None) -> Any:
        """
        Получение значения переменной окружения.

        Args:
            key: имя переменной
            default: значение по умолчанию

        Returns:
            Значение переменной или default
        """
        if self._structure is None:
            self._load_structure()

        for line_num, line_info in self._structure.items():
            if line_info.get('key') == key:
                return self._parse_value(line_info.get('value', ''))

        return default

    def _parse_value(self, value: str) -> Any:
        """
        Парсинг значения из строки.

        Args:
            value: строковое значение

        Returns:
            Распарсенное значение
        """
        if not value:
            return None

        # Boolean
        if value.upper() == 'TRUE':
            return True
        if value.upper() == 'FALSE':
            return False
        if value.upper() == 'NONE':
            return None

        # JSON object/array
        if value.startswith('{') or value.startswith('['):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass

        # Number
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            pass

        # String
        return value

    def _format_value(self, value: Any) -> str:
        """
        Форматирование значения для записи в .env.

        Args:
            value: значение любого типа

        Returns:
            Строковое представление
        """
        if value is None:
            return ''

        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'

        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)

        if isinstance(value, list):
            return json.dumps(value, ensure_ascii=False)

        if isinstance(value, (int, float)):
            return str(value)

        # String - экранируем спецсимволы
        return str(value).replace('"', '\"')

    def get_all(self) -> Dict[str, Any]:
        """
        Получение всех переменных из .env.

        Returns:
            Словарь {ключ: значение}
        """
        if self._structure is None:
            self._load_structure()

        result = {}
        for line_info in self._structure.values():
            if line_info.get('type') == 'key_value':
                key = line_info.get('key')
                value = line_info.get('value', '')
                result[key] = self._parse_value(value)

        return result

    def save(self, updates: Dict[str, Any]) -> bool:
        """
        Сохранение конфигурации в .env файл.
        Обновляет существующие значения, сохраняя комментарии.

        Args:
            updates: словарь с обновлениями {параметр: значение}

        Returns:
            True если успешно
        """
        if self._structure is None:
            self._load_structure()

        if not self._structure:
            logger.warning("Структура .env пуста, создаём новую")
            self._structure = {1: {'type': 'empty', 'content': '', 'original': ''}}

        # Обновляем существующие значения
        updated_keys = set()

        for line_num in sorted(self._structure.keys()):
            line_info = self._structure[line_num]

            if line_info.get('type') != 'key_value':
                continue

            key = line_info.get('key')
            if key in updates:
                new_value = self._format_value(updates[key])
                line_info['content'] = f'{key}={new_value}'
                line_info['original'] = f'{line_info["content"]}\n'
                line_info['value'] = new_value
                updated_keys.add(key)

        # Добавляем новые параметры (в конец файла)
        new_params = []
        for key, value in updates.items():
            if key not in updated_keys:
                formatted_value = self._format_value(value)
                new_line = f'\n# Добавлено через веб-интерфейс\n{key}={formatted_value}\n'
                new_params.append(new_line)

        if new_params:
            # Добавляем пустую строку перед новыми параметрами
            last_line_num = max(self._structure.keys())
            self._structure[last_line_num + 1] = {
                'type': 'empty',
                'content': '',
                'original': '\n'
            }
            for i, param in enumerate(new_params):
                line_num = last_line_num + 2 + i
                self._structure[line_num] = {
                    'type': 'key_value',
                    'key': param.split('=')[1].split('\n')[0] if '=' in param else '',
                    'content': param.strip(),
                    'original': param,
                    'value': param.split('=', 1)[1].strip() if '=' in param else ''
                }

        # Записываем файл
        return self._write_file()

    def _write_file(self) -> bool:
        """
        Запись структуры в файл.

        Returns:
            True если успешно
        """
        try:
            lines = []
            for line_num in sorted(self._structure.keys()):
                lines.append(self._structure[line_num]['original'])

            with open(self.env_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)

            logger.info(f"Конфигурация сохранена в {self.env_path}")
            return True

        except Exception as e:
            logger.error(f"Ошибка записи .env файла: {e}")
            return False

    def update_from_dict(self, config_dict: Dict[str, Any]) -> bool:
        """
        Обновление конфигурации из словаря с вложенной структурой.

        Args:
            config_dict: словарь с конфигурацией

        Returns:
            True если успешно
        """
        flat_updates = self._flatten_dict(config_dict)
        return self.save(flat_updates)

    def _flatten_dict(self, nested: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
        """
        Преобразование вложенного словаря в плоский.

        Args:
            nested: вложенный словарь
            prefix: префикс для ключей

        Returns:
            Плоский словарь
        """
        result = {}

        for key, value in nested.items():
            new_key = f"{prefix}_{key.upper()}" if prefix else key.upper()

            if isinstance(value, dict):
                result.update(self._flatten_dict(value, new_key))
            else:
                result[new_key] = value

        return result

    def get_section(self, section_name: str) -> Dict[str, Any]:
        """
        Получение секции конфигурации.

        Args:
            section_name: название секции (например: 'app', 'intervals')

        Returns:
            Словарь с параметрами секции
        """
        all_values = self.get_all()
        prefix = f"{section_name.upper()}_"

        section = {}
        for key, value in all_values.items():
            if key.startswith(prefix):
                new_key = key[len(prefix):].lower()
                section[new_key] = value

        return section

    def save_and_update_config(self, section: str, updates: Dict[str, Any], config) -> bool:
        """
        Сохранение конфигурации в .env и обновление глобального объекта config.

        Args:
            section: секция конфигурации (например: 'app', 'scheduler', 'parser_status', 'api')
            updates: словарь с обновлениями {параметр: значение}
            config: глобальный объект конфигурации

        Returns:
            True если успешно
        """
        # Преобразуем ключи в верхний регистр
        formatted_updates = {key.upper(): value for key, value in updates.items()}

        # Сохраняем в .env файл
        if not self.save(formatted_updates):
            return False

        # Преобразуем ключи в нижний регистр для config объекта
        # (атрибуты в классах AppConfig, APIConfig и т.д. в нижнем регистре)
        lowercase_updates = {key.lower(): value for key, value in updates.items()}

        # Обновляем in-memory config ОДИН раз после сохранения в файл
        config.update_config(section, lowercase_updates)

        return True

# Функция для быстрого доступа к конфигурации
def get_env_manager() -> EnvManager:
    """
    Получение экземпляра EnvManager.

    Returns:
        Экземпляр EnvManager
    """
    return EnvManager()
