create or replace package body RSB_TOOLS IS

-- Функция получения списка столбцов таблицы
function get_columnsList(tableName in varchar2, alias in varchar2)
return varchar2
is
 v_columnsList varchar2(32000);
 v_alias       varchar2(32000);
 cursor c_column is
   select cname from col where tname = upper(tableName) order by cname;
begin
  if alias is not null then
      v_alias := alias || '.';
  end if;
  for i in c_column loop
    v_columnsList := v_columnsList || ', ' || v_alias || i.cname;
  end loop;
  return substr(v_columnsList, 2);
end get_columnsList;

-- Процедура копирования данных из одной таблицы в другую с учетом владельцев
procedure copy_table_impl 
(
  p_owner_src varchar2
 ,p_tabname_src varchar2
 ,p_alias_src varchar2
 ,p_owner_dst varchar2
 ,p_tabname_dst varchar2
 ,p_alias_dst varchar2
 ,p_clause varchar2 default ' '
 ,p_col_ainc varchar2 default NULL
) IS
 v_cols varchar2(32767) := '';
 v_keynum integer;
 v_tr integer;
 v_eq boolean;
 e_no_tr exception;

 cursor c_col is
    select COLUMN_NAME from ALL_TAB_COLUMNS 
        where OWNER = upper(p_owner_dst)
        and TABLE_NAME = upper(p_tabname_dst) 
        order by TABLE_NAME;
  cursor sc_col is
    select COLUMN_NAME from ALL_TAB_COLUMNS 
        where OWNER = upper(p_owner_src)
        and TABLE_NAME = upper(p_tabname_src) 
        order by TABLE_NAME;
begin
/*    DBMS_OUTPUT.PUT_LINE(' p_owner_dst: <' || p_owner_dst || '>');
    DBMS_OUTPUT.PUT_LINE(' p_owner_src: <' || p_owner_src || '>');
    DBMS_OUTPUT.PUT_LINE(' v_cols dst:');
    for i in c_col loop
        DBMS_OUTPUT.PUT_LINE( i.COLUMN_NAME );
    end loop;
    DBMS_OUTPUT.PUT_LINE(' v_cols scr:');
    for j in sc_col loop
        DBMS_OUTPUT.PUT_LINE( j.COLUMN_NAME );
    end loop;*/
  for i in c_col loop
    v_eq := false;
    for j in sc_col loop
        v_eq := v_eq OR upper(i.COLUMN_NAME) = upper(j.COLUMN_NAME);
    end loop;
    if v_eq then
        v_cols := v_cols || ',' || i.COLUMN_NAME;
    end if;
  end loop;
  v_cols := substr(v_cols,2);
  --DBMS_OUTPUT.PUT_LINE('v_cols: '||v_cols); 
 if p_col_ainc is not null then
    select count(1) into v_tr from all_triggers
      where owner = p_owner_dst
       and table_name = UPPER(p_tabname_dst)
       and trigger_name like '%_AINC'
       and status = 'ENABLED';
    IF SQL%NOTFOUND THEN
      RAISE e_no_tr;
    END IF;
    v_cols := REGEXP_REPLACE ( v_cols, UPPER('(,{1}'||p_col_ainc||')|('||p_col_ainc||',{1})'), '' );
  end if;
  execute immediate 'insert into ' || p_owner_dst || '.' || p_tabname_dst || ' ' || p_alias_dst || ' (' || v_cols || ') ' || 'select ' || v_cols ||
     ' from ' || p_owner_src || '.' || p_tabname_src || ' ' || p_alias_src || ' ' || p_clause;
     
EXCEPTION
  WHEN e_no_tr THEN
    RAISE_APPLICATION_ERROR(-20002, 'Table ' || p_tabname_dst ||
     ' does not exist or does not have autoinc trigger or trigger is disabled');
end copy_table_impl;

-- Процедура копирования данных из одной таблицы в другую.
procedure copy_table 
(
  p_tabname_src varchar2
 ,p_tabname_dst varchar2
 ,p_clause varchar2 default ' '
 ,p_col_ainc varchar2 default NULL
) IS
v_owner_src varchar2(256) := '';
v_owner_dst varchar2(256) := '';
v_ntab_src varchar2(256) := '';
v_ntab_dst varchar2(256) := '';
v_alias_src varchar2(256) := '';
v_alias_dst varchar2(256) := '';
v_regex_tabname varchar2(256) := '(\s*"?\s*(\w*)\s*"?\.)?"?\s*(\w+)\s*"?\s*(\w*)\s*';
begin
    v_owner_src := REGEXP_REPLACE ( p_tabname_src, v_regex_tabname, '\2' );  
    v_owner_dst := REGEXP_REPLACE ( p_tabname_dst, v_regex_tabname, '\2' );
    v_ntab_src := REGEXP_REPLACE ( p_tabname_src, v_regex_tabname, '\3' ); 
    v_ntab_dst := REGEXP_REPLACE ( p_tabname_dst, v_regex_tabname, '\3' );
    v_alias_src := REGEXP_REPLACE ( p_tabname_src, v_regex_tabname, '\4' ); 
    v_alias_dst := REGEXP_REPLACE ( p_tabname_dst, v_regex_tabname, '\4' ); 
   
    if v_owner_src is null then
        select sys_context( 'userenv', 'current_schema' ) into v_owner_src from dual;
    end if;
    if v_owner_dst is null then
        select sys_context( 'userenv', 'current_schema' ) into v_owner_dst from dual;
    end if;
/*    
    DBMS_OUTPUT.PUT_LINE(' v_owner_src: <' || v_owner_src || '>');
    DBMS_OUTPUT.PUT_LINE(' v_owner_dst: <' || v_owner_dst || '>');
    DBMS_OUTPUT.PUT_LINE(' v_ntab_src: <' || v_ntab_src || '>');
    DBMS_OUTPUT.PUT_LINE(' v_ntab_dst: <' || v_ntab_dst || '>');   
    DBMS_OUTPUT.PUT_LINE(' v_alias_src: <' || v_alias_src || '>');
    DBMS_OUTPUT.PUT_LINE(' v_alias_dst: <' || v_alias_dst || '>');
*/
    copy_table_impl(v_owner_src, v_ntab_src, v_alias_src, v_owner_dst, v_ntab_dst, v_alias_dst, p_clause, p_col_ainc);
end copy_table;


-- Процедура переноса данных из одной таблицы в другую.
PROCEDURE merge_tables
(
  p_tabname_src VARCHAR2
 ,p_tabname_dst VARCHAR2
 ,p_clause      VARCHAR2
 ,p_col_ainc    VARCHAR2 DEFAULT NULL
 ,p_cols_notupd VARCHAR2 DEFAULT NULL
 ,p_parallel    INTEGER DEFAULT 0
) IS
 v_cols VARCHAR2(32767) := '';
 v_cols_alias VARCHAR2(32767) := '';
 v_cols_upd VARCHAR2(32767) := '';
 v_stmt VARCHAR2(32767) := '';
 v_tr INTEGER;
 e_no_tr EXCEPTION;
 CURSOR c_col IS
   SELECT cname FROM col WHERE tname = UPPER(p_tabname_dst) ORDER BY cname;
BEGIN
  FOR i IN c_col LOOP
    IF p_col_ainc IS NULL OR UPPER(i.cname) != UPPER(p_col_ainc) THEN
      v_cols := v_cols || ',' || i.cname;

      v_cols_alias := v_cols_alias || ',' || p_tabname_src || '.' || i.cname;

      IF p_cols_notupd IS NULL OR INSTR(',' || UPPER(p_cols_notupd) || ',', ',' || UPPER(i.cname) || ',') = 0  THEN
        v_cols_upd := v_cols_upd || ',' || p_tabname_dst || '.' || i.cname || '=' || p_tabname_src || '.' || i.cname;
      END IF;
    END IF;
  END LOOP;
  
  v_cols := SUBSTR(v_cols, 2);
  v_cols_alias := SUBSTR(v_cols_alias, 2);
  v_cols_upd := SUBSTR(v_cols_upd, 2);

  IF p_col_ainc IS NOT NULL THEN
    SELECT COUNT(1) INTO v_tr FROM user_triggers
      WHERE table_name = UPPER(p_tabname_dst)
       AND trigger_name like '%_AINC'
       AND status = 'ENABLED';
    IF SQL%NOTFOUND THEN
      RAISE e_no_tr;
    END IF;
  END IF;

  v_stmt := 'MERGE ' || CASE WHEN p_parallel = 0 THEN '' ELSE ' /*+ PARALLEL */ ' END || 
            '  INTO ' || p_tabname_dst || ' USING ' || p_tabname_src ||
            '  ON (' || p_clause || ') ' ||
            ' WHEN MATCHED THEN ' ||
            '   UPDATE SET ' || v_cols_upd ||
            ' WHEN NOT MATCHED THEN ' ||
            '   INSERT (' || v_cols || ') ' ||
            '   VALUES (' || v_cols_alias || ') ';

  --dbms_output.put_line(v_stmt);
  EXECUTE IMMEDIATE v_stmt;
EXCEPTION
  WHEN e_no_tr THEN
    RAISE_APPLICATION_ERROR(-20002, 'Table ' || p_tabname_dst ||
     ' does not exist or does not have autoinc trigger or trigger is disabled');
END merge_tables;


-- Процедура для сервиса файлов накопления, создает секционированную копию указанной таблицы.
procedure create_part_table(p_tabname varchar2, p_colname varchar2, p_value varchar2) IS
 e_no_tab exception;
 PRAGMA EXCEPTION_INIT (e_no_tab, -942);
begin
  execute immediate 'drop table ' || p_tabname || '_part';
  execute immediate 'create table ' || p_tabname || '_part partition by range (' || p_colname ||
    ') (partition to_del values less than (' || p_value ||
    '), partition to_sav values less than (MAXVALUE)) as select * from ' || p_tabname;
exception
  when e_no_tab then
     execute immediate 'create table ' || p_tabname || '_part partition by range (' || p_colname ||
        ') (partition to_del values less than (' || p_value ||
        '), partition to_sav values less than (MAXVALUE)) as select * from ' || p_tabname;
end create_part_table;


-- Процедура для сервиса файлов накопления, очищает выгруженные данные
-- без таблицы, созданной в procedure create_part_table работать не будет!
procedure clear_table (p_tabname varchar2) IS
 cursor c_ind is
   select index_name from user_indexes
      where table_name = UPPER(p_tabname) and index_type <> 'LOB';
begin
  execute immediate 'alter table ' || p_tabname || '_part exchange partition to_sav with table ' ||
       p_tabname || ' excluding indexes without validation';
  execute immediate 'drop table ' || p_tabname || '_part';
  for i in c_ind loop
    execute immediate 'alter index ' || i.index_name || ' rebuild';
  end loop;
end clear_table;


-- Процедура для переименования ключа в реестре настроек.
procedure rename_regkey (p_path varchar2, p_new_name varchar2)
as
  e_long_name exception;
  e_no_data exception;
begin
  if length(p_new_name) > 31 then
    raise e_long_name;
  end if;
  update dregparm_dbt set t_name = p_new_name
    where t_keyid = (
       select p.t_keyid from (
          select t_keyid, UPPER(substr(SYS_CONNECT_BY_PATH(t_name, '/'), 2)) path
            from dregparm_dbt
            start with t_parentid = 0
            connect by t_parentid = prior t_keyid) p
         where p.path = UPPER(translate(p_path, '\', '/')));
  if sql%notfound then
    raise e_no_data;
  end if;
  commit;
exception
  when e_no_data then
    RAISE_APPLICATION_ERROR(-20001, 'Cannot find specified path: ' || p_path);
  when e_long_name then
    RAISE_APPLICATION_ERROR(-20002, 'New name: ' || p_new_name || ' length must be less than 31 characters');
end;

-- Функция для нахождения ключа (точнее, его t_keyid) в реестре настроек.
-- Путь к настройке НЕ должен иметь '/' в начале и в конце, регистр не важен.
function find_regkey( p_path in varchar2 ) return integer
as
    v_keyid integer;
begin
    select p.t_keyid into v_keyid from (
        select t_keyid, UPPER( substr( SYS_CONNECT_BY_PATH( t_name, '/' ), 2 ) ) path
            from dregparm_dbt
            start with t_parentid = 0
            connect by t_parentid = prior t_keyid ) p
        where p.path = UPPER( translate( p_path, '\', '/' ) );
    return v_keyid;
exception
    when no_data_found then
        RAISE_APPLICATION_ERROR( -20001, 'Cannot find specified path: ' || p_path );
end;

-- Процедура для удаления ключа из реестра настроек.
-- Путь к настройке НЕ должен иметь '/' в начале и в конце, регистр не важен.
-- Случай отсутствия настройки обрабатывается "молча".
procedure remove_regkey( p_path in varchar2 )
as
    e_no_path exception;
    PRAGMA EXCEPTION_INIT( e_no_path, -20001 );

    v_keyid integer;
begin
    v_keyid := find_regkey( p_path );
    delete from dregparm_dbt
        where t_keyid = v_keyid;
exception
    when e_no_path then
        NULL;
end;

-- Процедура для перемещения ключа (или ветви) реестра настроек из одной ветви в другую.
-- p_oldpath - старый путь (ветвь и имя ключа), p_newbranch - новая ветвь.
-- Пути к настройкам НЕ должны иметь '/' в начале и в конце, регистр не важен.
procedure move_regkey( p_oldpath in varchar2, p_newbranch in varchar2 )
as
    -- исключения из find_regkey не ловим, пусть летят дальше
    v_keyid integer;
    v_branchid integer;
begin
    v_keyid := find_regkey( p_oldpath );
    v_branchid := find_regkey( p_newbranch );
    update dregparm_dbt set t_parentid = v_branchid
        where t_keyid = v_keyid;
end;

procedure put_rawline(p_line in varchar2)
as
    e_user exception;
    PRAGMA EXCEPTION_INIT(e_user, -20000);

    v_line varchar2(10000) := p_line;
begin
    while length(v_line) > 0
    loop
        begin
            dbms_output.put_line(substr(v_line, 1, 255));
        exception
            when e_user
                then NULL;
        end;

        v_line := substr(v_line, 256);
    end loop;
end;

  PROCEDURE dynamic_autonomous(stmt VARCHAR2)
  IS
    PRAGMA autonomous_transaction;
    BEGIN
    EXECUTE IMMEDIATE stmt;
    COMMIT;
  END;

-- Процедура для создания копии p_tabname_dst существующей таблицы p_tabname_src
-- Если в p_withrows передан 0, то записи копироваться не будут
-- В p_withrows также возвращаемое значение
procedure clone_table( p_tabname_src in varchar2, p_tabname_dst in varchar2, p_withrows in out integer )
IS
   stat integer;
BEGIN
   stat := 0;
   begin
      execute immediate 'drop table ' || p_tabname_dst;
      EXCEPTION WHEN OTHERS THEN NULL;
   end;
   begin
      if p_withrows = 0 then
         execute immediate 'create table ' || p_tabname_dst || ' as select * from ' || p_tabname_src || ' where 0 = 1';
     else
         execute immediate 'create table ' || p_tabname_dst || ' as select * from ' || p_tabname_src;
     end if;
      EXCEPTION WHEN OTHERS THEN stat := 1;
   end;
   p_withrows := stat;
END clone_table;


function getFunctionColumnExpression(p_table_name varchar2, p_index_name varchar2, p_column_position integer, p_default_expr varchar2)
return varchar2
is
  expr varchar2(32000);
begin
  select column_expression into expr
  from user_ind_expressions
  where table_name = p_table_name
    and index_name = p_index_name
    and column_position = p_column_position;
    
  return expr;
exception when no_data_found then
  return p_default_expr;
end getFunctionColumnExpression;


/**
 * Процедура для создания копии индексов для p_tabname_dst по существующим индексам таблицы p_tabname_src.
 * @param p_tabname_src  имя исходной таблицы
 * @param p_tabname_dst  имя таблицы копии
 */
procedure clone_table_indexes( p_tabname_src in varchar2, p_tabname_dst in varchar2 )
IS
  v_tabname_src user_indexes.index_name%TYPE DEFAULT UPPER(p_tabname_src);
  v_col_list VARCHAR2(32000);
  v_stmt VARCHAR2(32000);
BEGIN
  FOR indx IN (
      SELECT index_name, uniqueness, tablespace_name
        FROM user_indexes
        WHERE table_name = v_tabname_src
          AND index_name LIKE '%' || v_tabname_src || '%'
          AND generated = 'N'
        ORDER BY index_name  
    )
  LOOP
    --dbms_output.put_line(indx.index_name);
    
    SELECT LISTAGG(getFunctionColumnExpression(table_name, index_name, column_position, column_name) || ' ' || descend, ', ') 
                   WITHIN GROUP(ORDER BY column_position) INTO v_col_list
    FROM user_ind_columns c
    WHERE table_name = v_tabname_src
      AND index_name = indx.index_name
    GROUP BY index_name;
    
    --dbms_output.put_line('  ' || v_col_list);
    
    v_stmt := 'CREATE';
    IF indx.uniqueness = 'UNIQUE' THEN
      v_stmt := v_stmt || ' UNIQUE';
    END IF;
    v_stmt := v_stmt || ' INDEX ' || REPLACE(indx.index_name, v_tabname_src, UPPER(p_tabname_dst)) || ' ON ' || p_tabname_dst ||
        '(' || v_col_list || ')';
    IF indx.tablespace_name IS NOT NULL THEN
      v_stmt := v_stmt || ' TABLESPACE ' || indx.tablespace_name;
    END IF;
    
    --dbms_output.put_line('  ' || v_stmt);
    
    EXECUTE IMMEDIATE v_stmt;
  END LOOP;
  
END clone_table_indexes;

end RSB_TOOLS;
