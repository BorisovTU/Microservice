/**
 * Пакет для работы апгрейдера системы
 */
create or replace package RSB_TOOLS IS
-- Автор: Новиков
-- 17.12.03

/**
 * Функция получения списка столбцов таблицы
 * @param tableName  имя таблицы
 * @param alias      псевдоним
 * @return VARCHAR2
 */
function get_columnsList(tableName in varchar2, alias in varchar2) return varchar2;

/**
 * Расширенная процедура копирования таблиц. Учитывает имя владельца таблицы и псевдоним 
 * примеры вызова
 * rsb_tools.copy_table_impl('bankdistr', 'test', 'dst', 'cli', 'test', 'upg', null,'t_id') 
 * @param p_owner_src varchar2    имя владельца таблицы откуда копируем.
 * @param p_tabname_src varchar2  только имя таблицы откуда копируем
 * @param p_alias_src varchar2    псевдоним таблицы откуда копируем 
 * @param p_owner_dst varchar2    имя владельца таблицы куда копируем
 * @param p_tabname_dst varchar2  только имя таблицы куда копируем
 * @param p_alias_dst varchar2    псевдоним таблицы куда копируем 
 * @param p_clause varchar2 default ' '     можно задать условие where
 * @param p_col_ainc varchar2 default NULL  имя автоинкрементного столбца       
 */
procedure copy_table_impl 
(
  p_owner_src varchar2    -- имя владельца таблицы откуда копируем.
 ,p_tabname_src varchar2  -- только имя таблицы откуда копируем
 ,p_alias_src varchar2    -- псевдоним таблицы откуда копируем 
 ,p_owner_dst varchar2    -- имя владельца таблицы куда копируем
 ,p_tabname_dst varchar2  -- только имя таблицы куда копируем
 ,p_alias_dst varchar2    -- псевдоним таблицы куда копируем 
 ,p_clause varchar2 default ' '     -- можно задать условие where
 ,p_col_ainc varchar2 default NULL  -- имя автоинкрементного столбца
);

/**
 * Процедура копирования данных из одной таблицы в другую.
 * Разработана для скриптов апгрейдера, формирует одинаковый список столбцов для двух таблиц.
 * Примеры вызова:
 *   rsb_tools.copy_table('bankdistr.test','test',null,'t_id');
 *   rsb_tools.copy_table('bankdistr.test','test','where name=''sasha'''); 
 *   Обратите внимание на кавычки!
 * @param p_tabname_src  имя таблицы откуда копируем
 * @param p_tabname_dst  имя таблицы куда копируем
 * @param p_clause       можно задать условие where
 * @param p_col_ainc     имя автоинкрементного столбца
 */
procedure copy_table 
(
  p_tabname_src varchar2     -- Откуда копируем
 ,p_tabname_dst varchar2     -- Куда копируем
 ,p_clause      varchar2 default ' ' -- Можно задать условие where
 ,p_col_ainc    varchar2 default NULL -- Имя автоинкрементного столбца
);

/**
 * Процедура слияния данных из одной таблицы в другую.
 * Формирует одинаковый список столбцов для двух таблиц.
 * Примеры вызова:
 *   rsb_tools.merge_tables('das_addrobj_tmp','das_addrobj_dbt','','t_id');
 * @param p_tabname_src  имя таблицы откуда переносим
 * @param p_tabname_dst  имя таблицы куда переносим
 * @param p_clause       нужно задать условие ON
 * @param p_col_ainc     имя автоинкрементного столбца
 * @param p_cols_notupd  имена не обновляемых столбцов через запятую
 * @param p_parallel     выполнять параллельно
 */
PROCEDURE merge_tables 
(
  p_tabname_src VARCHAR2 -- Откуда переносим
 ,p_tabname_dst VARCHAR2 -- Куда переносим
 ,p_clause   VARCHAR2 -- нужно задать условие ON
 ,p_col_ainc    VARCHAR2 DEFAULT NULL -- Имя автоинкрементного столбца
 ,p_cols_notupd VARCHAR2 DEFAULT NULL -- Имена не обновляемых столбцов через запятую
 ,p_parallel    INTEGER DEFAULT 0 -- Выполнять параллельно
);

/**
 * Процедура для сервиса файлов накопления, создает секционированную копию указанной таблицы.
 * Примеры вызова:
 *   rsb_tools.create_part_table('test','id',5);
 *   exec rsb_tools.create_part_table('test','day','to_date(''26-12-2003'', ''DD-MM-YYYY'')');
 *   обратите внимание на кавычки и формат даты - 4 цифры для года обязательно!
 * @param p_tabname  имя таблицы
 * @param p_colname  стобец по которому секционируем
 * @param p_value    значение, все что меньше попадет в партицию to_del
 * @return
 */
procedure create_part_table (p_tabname varchar2,  -- имя таблицы
                             p_colname varchar2,  -- стобец по которому секционируем
                             p_value   varchar2   -- значение, все что меньше попадет в партицию to_del
                            );

/**
 * Процедура для сервиса файлов накопления, очищает выгруженные данные.
 * Без таблицы, созданной в procedure create_part_table работать не будет!
 * @param p_tabname  имя таблицы
 */
procedure clear_table (p_tabname varchar2);

/**
 * Процедура для переименования ключа в реестре настроек.
 * Путь к настройке НЕ должен иметь '/' в начале и в конце, новое имя не длинее 31 символа, регистр не важен.
 * Пример вызова:
 *   rsb_tools.rename_regkey('cb/REPORT/FI_RATE/MACRONAME', 'MACRONAME11');
 * @param p_path      полный путь настройки
 * @param p_new_name  новое имя настройки
 */
procedure rename_regkey (p_path varchar2, p_new_name varchar2);

/**
 * Функция для нахождения ключа (точнее, его t_keyid) в реестре настроек.
 * Путь к настройке НЕ должен иметь '/' в начале и в конце, регистр не важен.
 * @param p_path  полный путь настройки
 * @return INTEGER
 */
function find_regkey( p_path in varchar2 ) return integer;

/**
 * Процедура для удаления ключа из реестра настроек.
 * Путь к настройке НЕ должен иметь '/' в начале и в конце, регистр не важен.
 * Случай отсутствия настройки обрабатывается "молча".
 * @param p_path  полный путь настройки
 */
procedure remove_regkey( p_path in varchar2 );

/**
 * Процедура для перемещения ключа (или ветви) реестра настроек из одной ветви в другую.
 * Пути к настройкам НЕ должны иметь '/' в начале и в конце, регистр не важен.
 * @param p_oldpath    старый путь (ветвь и имя ключа)
 * @param p_newbranch  новая ветвь
 */
procedure move_regkey( p_oldpath in varchar2, p_newbranch in varchar2 );

/**
 * Процедура для вывода длинной строки в dbms_output путем разделения на строки длиной не более 255 символов.
 * @param p_line  строка для вывода в dbms_output
 */
procedure put_rawline(p_line in varchar2);

/**
 * Процедура для выполнения команды в режиме автономной транзакции
 * @param stmt  команда для выполнения
 */
PROCEDURE dynamic_autonomous(stmt VARCHAR2);

/**
 * Процедура для создания копии p_tabname_dst существующей таблицы p_tabname_src.
 * Если в p_withrows передан 0, то записи копироваться не будут.                 
 * В p_withrows также возвращаемое значение.                                     
 * @param p_tabname_src  имя исходной таблицы
 * @param p_tabname_dst  имя таблицы копии
 * @param p_withrows     кол-во записей для копирования
 */
procedure clone_table( p_tabname_src in varchar2, p_tabname_dst in varchar2, p_withrows in out integer );

-- should be package private
function getFunctionColumnExpression(p_table_name varchar2, p_index_name varchar2, p_column_position integer, p_default_expr varchar2)
return varchar2;

/**
 * Процедура для создания копии индексов для p_tabname_dst по существующим индексам таблицы p_tabname_src.
 * @param p_tabname_src  имя исходной таблицы
 * @param p_tabname_dst  имя таблицы копии
 */
procedure clone_table_indexes( p_tabname_src in varchar2, p_tabname_dst in varchar2 );

end RSB_TOOLS;
