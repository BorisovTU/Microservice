create or replace package body it_spi_import
is
 /**************************************************************************************************\
  BIQ-6664 / Загрузка справочника платежных инструкций СПИ
  **************************************************************************************************
  Изменения:
  ---------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                             Описание 
  ----------  ---------------  ------------------------------   -------------------------------------
  31.01.2022  Мелихова О.С.    BIQ-6664 CCBO-506                Создание
 \**************************************************************************************************/
  s_result_text clob;

  c_UNKNOWN     constant varchar2(50) := 'UNKNOWN';
  c_ERROR       constant varchar2(50) := 'ERROR';
  c_UPDATE      constant varchar2(50) := 'UPDATE';
  c_INSERT      constant varchar2(50) := 'INSERT'; 
       
  --Запуск процесса  
  procedure execute_process(p_xml         in clob,
                            p_from_system in varchar2) --принимаем файл в XML формате
  is
    v_xml_clob              clob;
    v_xml                   xmltype;
    v_result_text           clob;   

    v_key_bit               number;
    v_data_row_count        number;
    v_spi_exist_b           boolean; 
    v_find_b                boolean := false;
    v_incorret_file_format  boolean;
    v_id_file               number;
    v_file_dir              varchar2(250);
    v_file_name             varchar2(250);
    v_create_user           varchar2(250); 
    v_from_module           varchar2(250);  
    
    v_fi_code               varchar2(50);
    v_list_spi              varchar2(50); 
    v_obj_cft               number;
    v_partyid               number;
    v_bank_partyid          number;
    v_corr_bank_partyid     number;
    v_max_elem              number; 
    v_fiid                  number; 
    
    v_itt_spi_import_row    itt_spi_import%rowtype; 
    v_dsettacc_dbt_row      dsettacc_dbt%rowtype;
  begin    
    it_log.log('begin');
    it_log.log(p_msg => 'XML file: ' ,p_msg_clob => p_xml);
    it_error.clear_error_stack;    

    savepoint sp;
    
    s_result_text := null;
    v_xml_clob := p_xml;
   
    if v_xml_clob is null then
      raise_application_error(-20001,'Содержимое файла пусто');
    end if; 
          
    --удалим непечатаемые символы
    select regexp_replace(v_xml_clob, '[[:cntrl:]]') 
      into v_xml_clob  
      from dual;
       
    -- Undefined заменим на null 
    select replace(v_xml_clob, 'Undefined') 
      into v_xml_clob  
      from dual; 
            
    begin   
      v_xml := XMLTYPE(v_xml_clob);
    exception
      when others then 
        raise_application_error(-20002,'Некорректный формат файла');
    end;
   
    --it_log.log(p_msg_clob => v_xml_clob);       
   
    --читаем атрибуты файла    
    select extractvalue(v_xml,'/XML/FILE_DIR/text()')
         , extractvalue(v_xml,'/XML/FILE_NAME/text()') 
         , extractvalue(v_xml,'/XML/CREATE_USER/text()')  
         , extractvalue(v_xml,'/XML/FROM_MODULE/text()')  
      into v_file_dir,v_file_name, v_create_user, v_from_module 
      from dual;  
             
    v_id_file := it_file.insert_file(p_file_dir    => v_file_dir,
                                     p_file_name   => v_file_name,
                                     p_file_clob   => p_xml,
                                     p_from_system => p_from_system,
                                     p_from_module => v_from_module,
                                     p_to_system   => it_file.C_SOFR_DB,
                                     p_to_module   => $$PLSQL_UNIT,
                                     p_create_user => v_create_user);
       
    it_log.log('file_dir='||v_file_dir||'; file_name='||v_file_name ||'; create_user ='||v_create_user||';');
    
    
    v_data_row_count := 0;
     
    for cc in (select tf.column_value.extract('/ROW/@excel_rownum').getnumberval() as  excel_rownum    
                    , trim(extractValue(tf.column_value,'/ROW/PARENTGROUP'))    as  parentgroup
                    , trim(extractValue(tf.column_value,'/ROW/GROUPNAME'))      as  groupname 
                    , trim(extractValue(tf.column_value,'/ROW/FULLNAME'))       as  fullname 
                    , trim(extractValue(tf.column_value,'/ROW/ACCOUNT_NAME'))   as  account_name  
                    , trim(extractValue(tf.column_value,'/ROW/INSTRUMENT'))     as  instrument 
                    , trim(extractValue(tf.column_value,'/ROW/SI_EXTERNAL'))    as  si_external
                    , trim(extractValue(tf.column_value,'/ROW/SI_INTERNAL'))    as  si_internal 
                    , trim(extractValue(tf.column_value,'/ROW/IS_DEFAULT'))     as  is_default
                    , trim(extractValue(tf.column_value,'/ROW/DISABLED'))       as  disabled
                    , trim(extractValue(tf.column_value,'/ROW/CFT_FILE_NAME'))  as  cft_file_name  
                    , trim(replace(upper(extractValue(tf.column_value,'/ROW/CURRENCY')),'RUR','RUB'))as  currency
                    , trim(extractValue(tf.column_value,'/ROW/ACCOUNT'))        as  account 
                    , trim(extractValue(tf.column_value,'/ROW/BANK_NAME'))      as  bank_name 
                    , trim(extractValue(tf.column_value,'/ROW/BIC'))            as  bic 
                    , trim(extractValue(tf.column_value,'/ROW/CORR_ACCOUNT'))   as  corr_account 
                    , trim(extractValue(tf.column_value,'/ROW/CORR_BANK_NAME')) as  corr_bank_name 
                    , trim(extractValue(tf.column_value,'/ROW/CORR_BIC'))       as  corr_bic 
                    , trim(extractValue(tf.column_value,'/ROW/SWIFT'))          as  swift 
                    , trim(extractValue(tf.column_value,'/ROW/ASUDR'))          as  asudr 
                    , trim(extractValue(tf.column_value,'/ROW/SPI_ID'))         as  spi_id 
                    , cast(null as varchar2(50))                               as  code_cft                  
                 from table(xmlsequence(extract(v_xml,'/XML/ROWSET/ROW'))) tf     
               )
    loop      
      it_log.log('loop cc.excel_rownum='||cc.excel_rownum||';');
      
      v_find_b    := true;
      
      v_itt_spi_import_row := null;
      v_itt_spi_import_row.result_status := c_UNKNOWN;
      
      v_dsettacc_dbt_row  := null;
    
      v_fi_code           := null;
      v_list_spi          := null;
      
      v_max_elem          := 0;
      v_obj_cft           := 0;
      v_key_bit           := 0;
      
      v_partyid           := null;
      v_bank_partyid      := null;
      v_corr_bank_partyid := null;
      
      v_spi_exist_b       := false;    
     
      begin         
        if cc.excel_rownum = 1 --пропускаем первую строку в Excel
           /*--пропускаем строку с группировкой--*/
           OR cc.excel_rownum > 2
              and cc.fullname is not null
              and coalesce(cc.parentgroup, cc.groupname) is null
           /*------------------------------------*/     
        then
          it_log.log('1 continue  cc.excel_rownum='||cc.excel_rownum||';');
          continue; 
        end if;
        
        v_itt_spi_import_row.id_spi_import        := its_main.nextval(); 
        v_itt_spi_import_row.excel_rownum         := substr(cc.excel_rownum,1,1000);
        v_itt_spi_import_row.id_file              := substr(v_id_file,1,1000);
        v_itt_spi_import_row.parentgroup          := substr(cc.parentgroup,1,1000);
        v_itt_spi_import_row.groupname            := substr(cc.groupname,1,1000);
        v_itt_spi_import_row.fullname             := substr(cc.fullname,1,1000);
        v_itt_spi_import_row.account_name         := substr(cc.account_name,1,1000);
        v_itt_spi_import_row.instrument           := substr(cc.instrument,1,1000);
        v_itt_spi_import_row.si_external          := substr(cc.si_external,1,1000);
        v_itt_spi_import_row.si_internal          := substr(cc.si_internal,1,1000);
        v_itt_spi_import_row.is_default           := substr(cc.is_default,1,1000);
        v_itt_spi_import_row.disabled             := substr(cc.disabled,1,1000);
        v_itt_spi_import_row.cft_file_name        := substr(cc.cft_file_name,1,1000);
        v_itt_spi_import_row.currency             := substr(cc.currency,1,1000);
        v_itt_spi_import_row.account_no           := substr(cc.account,1,1000);
        v_itt_spi_import_row.bank_name            := substr(cc.bank_name,1,1000);
        v_itt_spi_import_row.bic                  := substr(cc.bic,1,1000);
        v_itt_spi_import_row.corr_account         := substr(cc.corr_account,1,1000);
        v_itt_spi_import_row.corr_bank_name       := substr(cc.corr_bank_name,1,1000);
        v_itt_spi_import_row.corr_bic             := substr(cc.corr_bic,1,1000);
        v_itt_spi_import_row.swift                := substr(cc.swift,1,1000);
        v_itt_spi_import_row.asudr                := substr(cc.asudr,1,1000);
        v_itt_spi_import_row.spi_id               := substr(cc.spi_id,1,1000);
        v_itt_spi_import_row.create_sysdate       := sysdate;

          
        /*1.	В строке заголовка отсутствует хотя бы одно поле из списка 
        (?GROUPNAME?, ? SI_INTERNAL?, ?Валюта счета?, ?Р/С (20 знаков)?, ?БИК?, ?К/С?, ?SWIFT?, ?Корректный идентификатор СПИ?), то
        a.	Добавить в протокол запись: ?Некорректный формат файла.?
        b.	Завершить сценарий обработки файла;
        c.	Отобразить протокол;*/

        if cc.excel_rownum = 2 then
          if cc.groupname is null or cc.si_internal is null or cc.currency is null 
             or cc.account is null or cc.bic is null or cc.corr_account is null  or cc.swift is null   
          then
            v_itt_spi_import_row.result_status := c_ERROR; 
            v_itt_spi_import_row.result_msg    := 'Некорректный формат файла.';
            v_incorret_file_format := true; --заголовка нет или он некорректный - завершим работу алгоритма на этой строке
          else
            it_log.log('2 continue  cc.excel_rownum='||cc.excel_rownum||';');
            continue; --пропускаем строку с корректным заголовком  
          end if;   
        end if;  
        
        --Начинаем анализ строк с данными
        
        if cc.excel_rownum > 2 
           and cc.account is null  
        then
          v_itt_spi_import_row.result_status := c_ERROR; 
          v_itt_spi_import_row.result_msg    := 'Отсутствует "Р/сч".';
          it_log.log(' v_itt_spi_import_row.result_status '|| v_itt_spi_import_row.result_status ||';');
        end if;

        if v_itt_spi_import_row.result_status != c_ERROR then 
          v_data_row_count := v_data_row_count + 1;
          --значение идентификатора ЦФТ
          cc.code_cft := substr(cc.groupname, instr (cc.groupname, '_', -1)+1);   
         
          --Список проверок СПИ
          --
           
          if cc.currency = 'RUB' then                   
            --1 Поле 11 ("Валюта счета")= 'RUB' И значение поля 12 ("Р/С (20 знаков)") 
            --содержит значение, не соответствующее регулярному выражению '^\d{20}$'           
            if regexp_substr(cc.account,'^\d{20}$') is null then   
              v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Поле "Р/сч" содержит некорректное значение;';
            end if;
            
            --2.Поле 14 ("БИК") не содержит значение
            if cc.bic is null then
              v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Поле "БИК банка" не содержит значение;';            
            --3.Поле 11 ("Валюта счета") = ?RUB? И значение поля 14 ("БИК") проверяем на соответствие регулярному выражению ?^\d{9}$?        
            elsif regexp_substr(cc.bic,'^\d{9}$') is null then
              v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' В поле "БИК банка" указано невалидное значение;';
            else 
              --4.Значение поля 14 ("БИК") невозможно найти в БИК-ах банков РФ
              select max(d.t_objectid)
                into v_bank_partyid
                from dobjcode_dbt d 
               where d.t_objecttype = 3 --субъект экономики
                 and d.t_codekind = 3   --БИК банка 
                 and d.t_code = cc.bic 
                 and t_state = 0;         
          
              if v_bank_partyid is null then
                v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Банк по значению в поле "БИК банка" не найден;';          
              end if;  
            end if;
            if v_itt_spi_import_row.result_msg is not null then
              v_itt_spi_import_row.result_status := c_ERROR; 
            end if;                
          end if; 
        end if;  
          
        if v_itt_spi_import_row.result_status != c_ERROR then
          --5.Поле 11 ("Валюта счета") = ?RUB? И значение ключевого разряда (9 символ) расчетного счета (поле 12) не соответствует расчетному  БИК-у банка           
          select substr( substr(
                 to_char(
                 to_number(substr(cc.bic,7,1)) * 7 +
                 to_number(substr(cc.bic,8,1)) * 1 +
                 to_number(substr(cc.bic,9,1)) * 3 +
                 to_number(substr(cc.account,1,1)) * 7 +
                 to_number(substr(cc.account,2,1)) * 1 +
                 to_number(substr(cc.account,3,1)) * 3 +
                 to_number(substr(cc.account,4,1)) * 7 +
                 to_number(substr(cc.account,5,1)) * 1 +
                 to_number(substr(cc.account,6,1)) * 3 +
                 to_number(substr(cc.account,7,1)) * 7 +
                 to_number(substr(cc.account,8,1)) * 1 +
                 --to_number(substr('30101810700000000000',9,1)) * 3 +
                 to_number(substr(cc.account,10,1)) * 7 +
                 to_number(substr(cc.account,11,1)) * 1 +
                 to_number(substr(cc.account,12,1)) * 3 +
                 to_number(substr(cc.account,13,1)) * 7 +
                 to_number(substr(cc.account,14,1)) * 1 +
                 to_number(substr(cc.account,15,1)) * 3 +
                 to_number(substr(cc.account,16,1)) * 7 +
                 to_number(substr(cc.account,17,1)) * 1 +
                 to_number(substr(cc.account,18,1)) * 3 +
                 to_number(substr(cc.account,19,1)) * 7 +
                 to_number(substr(cc.account,20,1)) * 1
                 ),-1,1) * 3, -1, 1) as key_bit
           into v_key_bit    
           from dual;

          if substr(cc.account,9,1) != v_key_bit then
            v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Ключевой разряд в поле "Р/сч" не соответствует БИК-у банка;';          
          end if;
        end if;               
       
        --6  Поле 11 ("Валюта счета") содержит невалидный код валюты
        --поиск по таблице Финансовый инструмент
        select max(dfi.t_fiid) 
          into v_fiid
          from dfininstr_dbt dfi 
         where dfi.t_fi_kind = 1   --Номер вида инструмента
           and dfi.t_avoirkind = 0 --Номер подвида инструмента
           and dfi.t_ccy = cc.currency 
           and dfi.t_isclosed <> chr(88);--не закрыт                   
         
        if v_fiid is null then
          v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Поле "Валюта счета" содержит некорректное значение;';          
        end if; 

        if cc.currency = 'RUB' then
          select max(d.t_objectid)
            into v_corr_bank_partyid
            from dobjcode_dbt d 
           where d.t_objecttype = 3 --субъект экономики
             and d.t_codekind = 3   --БИК банка 
             and d.t_code = cc.corr_bic 
             and t_state = 0;         
        else  
          --7.Поле 11 ("Валюта счета") <> ?RUB? И поле 15 ("К/С") не заполнено Не указан к/сч банка в банке-корреспонденте.
          if cc.corr_account is null then
            v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Не указан к/сч банка в банке-корреспонденте;'; 
          end if;   
            
          --8.Поле 11 ("Валюта счета") <> ?RUB? И поле 18 ("SWIFT") не заполнено 
          if cc.swift is null then
            v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Не указан SWIFT банка-корреспондента в ин.валюте;'; 
          else  
            --9.Поле 11 ("Валюта счета") <> ?RUB? И значение поля 18 ("SWIFT") невозможно найти в SWIFT-ах банков-корреспондентов
            select max(d.t_objectid)
              into v_corr_bank_partyid
              from dobjcode_dbt d 
             where d.t_objecttype = 3 --субъект экономики
               and d.t_codekind = 6 --BIC ISO (SWIFT)
               and d.t_code = cc.swift 
               and d.t_state = 0;   

            if v_corr_bank_partyid is null then
              v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Значение SWIFT банка-корреспондента содержит невалидное значение;'; 
            end if; 
          end if;                         
        end if; -- if cc.currency = 'RUB'
                                
          --10 В поле 2 ("GROUPNAME") не указано значение  
          if cc.groupname is null then
            v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' В поле "GROUPNAME" не указано значение;'; 
          end if;
           
          --11 В поле 7 ("SI_INTERNAL") И в поле 20 ("Корректный идентификатор СПИ") не указано значение 
          if nvl(cc.si_internal, cc.spi_id) is null then
            v_itt_spi_import_row.result_msg := v_itt_spi_import_row.result_msg || ' Отсутствует значение идентификатора СПИ в полях SI_INTERNAL и Корректный идентификатор СПИ;'; 
          end if;
                 
          
          if v_itt_spi_import_row.result_msg is not null then
            v_itt_spi_import_row.result_status     := c_ERROR;                                            
          end if;                                                                                                                  
               
        --Продолжаем обработку если при проверке отсутсвуют ошибки       
       
        if v_itt_spi_import_row.result_status != c_ERROR then       
          
          --проверить наличие в классификаторе идентификаторов СПИ значения из поля 
          select max(d.t_code)
            into v_list_spi 
            from dllvalues_dbt d  --Значение справочника
           where d.t_list = 1080 --Инструмент СПИ
             and trim(upper(d.t_code)) = upper(nvl(v_itt_spi_import_row.spi_id, v_itt_spi_import_row.si_internal));             
          
          it_log.log('v_list_spi='||v_list_spi||';'); 
          
          --если значение отсутствует, то добавить в классификатор идентификаторов СПИ значение поля 20;
          if v_list_spi is null then  
            begin              
            select max(d.t_element)+1 --инкремент
              into v_max_elem 
              from dllvalues_dbt d  --Значение справочника 
             where d.t_list = 1080; --из ТЗ
                  
            insert into dllvalues_dbt
            (  t_list
             , t_element
             , t_code
             , t_name
             , t_flag
             , t_note
             )
            values
            (  1080
             , v_max_elem
             , nvl(v_itt_spi_import_row.spi_id, v_itt_spi_import_row.si_internal)
             , nvl(v_itt_spi_import_row.spi_id, v_itt_spi_import_row.si_internal)
             , 0
             , nvl(v_itt_spi_import_row.spi_id, v_itt_spi_import_row.si_internal) 
             );
            
            v_itt_spi_import_row.t_list := 1080;
            v_itt_spi_import_row.t_element := v_max_elem;  
            exception
              when others then
                v_itt_spi_import_row.result_msg    := ' ошибка сохранения справочника СПИ СОФР. '||sqlerrm;
                v_itt_spi_import_row.result_status := c_ERROR;
                it_log.log(v_itt_spi_import_row.result_msg, it_log.C_MSG_TYPE__MSG);
            end;
          end if;  
          
        end if;         

        if v_itt_spi_import_row.result_status != c_ERROR then                                     
          --поиск идентификатора субъекта по идентификатору ЦФТ
          begin          
            select d.t_objectid 
              into v_partyid 
              from dobjcode_dbt d 
             where d.t_objecttype = 3 --субъект
               and d.t_codekind = 101 --ЦФТ
               and d.t_code = cc.code_cft 
               and d.t_state = 0; 
            it_log.log('v_partyid='||v_partyid||';');  
          exception
            when no_data_found then
              v_itt_spi_import_row.result_status := c_ERROR;
              v_itt_spi_import_row.result_msg := ' Субъект с указанным идентификатором ЦФТ {'||cc.code_cft||'} не найден.";';
              it_log.log(v_itt_spi_import_row.result_msg, it_log.C_MSG_TYPE__MSG);
          end;
        end if;


        if v_itt_spi_import_row.result_status != c_ERROR then       
          -- a.  Обновить СПИ субъекта в соответствии с правилами парсинга, приведенными в Таблица 2 //dsettacc_dbt
          -- если запись в таблице dsettacc_dbt отсутствует тогда выполняем вставку строки
          v_spi_exist_b  := false; --признак существования СПИ в справочнике СПИ
          it_log.log('v_partyid '||v_partyid||'v_itt_spi_import_row.spi_id='||v_itt_spi_import_row.spi_id||', v_itt_spi_import_row.si_internal='||v_itt_spi_import_row.si_internal||';');  
          it_log.log(nvl(v_itt_spi_import_row.spi_id, v_itt_spi_import_row.si_internal));
          begin           
            select *
              into v_dsettacc_dbt_row
              from dsettacc_dbt t --Счет для расчетов с субъектами
             where t.t_partyid = v_partyid 
               and trim(upper(t.t_spi_ident)) = upper(nvl(v_itt_spi_import_row.spi_id, v_itt_spi_import_row.si_internal));
                 
             v_spi_exist_b := true;   --строка таблицы СПИ dsettacc_dbt будет обновлена 
             it_log.log('строка таблицы СПИ dsettacc_dbt будет обновлена t_settaccid='||v_dsettacc_dbt_row.t_settaccid||';');  

          exception   
            when no_data_found then
              v_spi_exist_b := false;   --строка таблицы СПИ dsettacc_dbt будет вставлена;  
              it_log.log('строка таблицы СПИ dsettacc_dbt будет вставлена t_settaccid='||v_dsettacc_dbt_row.t_settaccid||';');  

            when too_many_rows then
              raise_application_error(-20004,'В справочнике СПИ (dsettacc_dbt) для субъекта t_partyid='||v_partyid||' найдено более одной строки с t_spi_ident='||nvl(v_itt_spi_import_row.spi_id, v_itt_spi_import_row.si_internal)||'.');     
              it_log.log('В справочнике СПИ (dsettacc_dbt) для су ;');  

          end; 
           
          --
          ------START----
          --                            
          v_dsettacc_dbt_row.t_bankid       := v_bank_partyid; --t_bankid     Заполнять значением идентификатора субъекта банка
          v_dsettacc_dbt_row.t_chapter      := 0;              --t_chapter       Заполняется константой ?0?
          v_dsettacc_dbt_row.t_account      := cc.account;     --t_account       Заполняется значением поля 12 ("Р/С (20 знаков)"
          v_dsettacc_dbt_row.t_bankcodekind := 3;              --t_bankcodekind  Заполняется константой ?3?          
          v_dsettacc_dbt_row.t_bankcode     := cc.bic;         --t_bankcode      Заполняется значением поля 14 ("БИК")
                      
          --t_fiid     Заполняется идентификатором валюты по внутрисистемному справочнику
          v_dsettacc_dbt_row.t_fiid := v_fiid;
            
          --t_inn     Заполняется значением ИНН субъекта
          select max(d.t_code) 
            into v_dsettacc_dbt_row.t_inn
            from dobjcode_dbt d 
           where d.t_objecttype = 3         --субъект экономики
             and d.t_codekind = 16          --ИНН
             and d.t_objectid = v_partyid;--'{идентификатор_субъекта}';

          --t_recname  Заполняется наименованием субъекта 
          select p.t_name
            into v_dsettacc_dbt_row.t_recname
            from dparty_dbt p
           where p.t_partyid = v_partyid;--'{идентификатор_субъекта}';                                               

          --t_bankname     Заполнять значением наименованием банка
          select p.t_name   
            into v_dsettacc_dbt_row.t_bankname
            from dparty_dbt p
           where p.t_partyid = v_bank_partyid;--'{идентификатор_субъекта_банка}';                                               
                         
          --t_bankcorrid    Заполняется идентификатором банка-корреспондента:
          --для реквизитов в рублях определяется поиском по значению поля 17 ("БИК") 
          --для реквизитов в инвалюте определяется поиском по значению поля 18 ("SWIFT")  
          v_dsettacc_dbt_row.t_bankcorrid := v_corr_bank_partyid;
             
          --t_bankcorrname    Заполняется значением наименования банка-корреспондента:
          --? для реквизитов в рублях определяется поиском по полю 17 ("БИК")
          select max(dp.t_name) 
            into v_dsettacc_dbt_row.t_bankcorrname
            from dparty_dbt dp 
           where dp.t_partyid = v_dsettacc_dbt_row.t_bankcorrid; 
          
          if cc.currency = 'RUB' then
            --t_corracc    ? Для реквизитов в рублях заполняется значением коррсчета поиском по значению поля 14 ("БИК")
            --?  Для реквизитов в инвалюте не заполняется
            begin 
              select db.t_coracc 
                into v_dsettacc_dbt_row.t_corracc
                from dbankdprt_dbt db 
               where db.t_checkdata = cc.bic --'{значение_поля_14 (БИК)}') 
                 and db.t_partyid = v_bank_partyid
                 ;
            exception
              when no_data_found then
                v_itt_spi_import_row.result_status := c_ERROR;
                v_itt_spi_import_row.result_msg := ' В справочнике Банков (dbankdprt_dbt) не найден Банк c t_checkdata='||cc.bic||' (БИК) и t_partyid = '||v_bank_partyid||';'; 
                it_log.log(v_itt_spi_import_row.result_msg, it_log.C_MSG_TYPE__MSG);
            end;
          end if;  
        end if;
          
        if v_itt_spi_import_row.result_status != c_ERROR then       
          if cc.currency = 'RUB' then
            
             --t_bankcorrcode     Заполняется кодом БИК/SWIFT банка-корреспондента:
             --?  для реквизитов в рублях заполняется значением поля 17 ("БИК")
             --?  для реквизитов в инвалюте заполняется значением поля 18 ("SWIFT")
             v_dsettacc_dbt_row.t_bankcorrcode     := cc.corr_bic;
             
             --t_bankcorrcodekind   Заполняется идентификатором кода БИК/SWIFT:
             --?  для реквизитов в рублях заполняется значением ?3?
             --?  для реквизитов в инвалюте заполняется значением ?6?   
             v_dsettacc_dbt_row.t_bankcorrcodekind := 3;
             
          else    
             v_dsettacc_dbt_row.t_bankcorrcode     := cc.swift; 
             v_dsettacc_dbt_row.t_bankcorrcodekind := 6;                      
          end if;          
           
          if  v_spi_exist_b then --обновление записи    
            v_itt_spi_import_row.t_partyid := v_partyid;
            v_itt_spi_import_row.t_settaccid := v_dsettacc_dbt_row.t_settaccid;
            v_itt_spi_import_row.result_status := c_UPDATE;
            v_itt_spi_import_row.result_msg := ' Платежная инструкция обновлена в СПИ;';    
            begin 
              update dsettacc_dbt db
                 set t_bankid            = v_dsettacc_dbt_row.t_bankid,
                     t_fiid              = v_dsettacc_dbt_row.t_fiid,
                     t_chapter           = v_dsettacc_dbt_row.t_chapter,
                     t_account           = v_dsettacc_dbt_row.t_account,
                     t_inn               = v_dsettacc_dbt_row.t_inn,
                     t_recname           = v_dsettacc_dbt_row.t_recname,
                     t_bankcodekind      = v_dsettacc_dbt_row.t_bankcodekind,
                     t_bankcode          = v_dsettacc_dbt_row.t_bankcode,
                     t_bankname          = v_dsettacc_dbt_row.t_bankname,
                     t_bankcorrname      = v_dsettacc_dbt_row.t_bankcorrname,
                     t_bankcorrid        = v_dsettacc_dbt_row.t_bankcorrid,            
                     t_bankcorrcodekind  = v_dsettacc_dbt_row.t_bankcorrcodekind,
                     t_bankcorrcode      = v_dsettacc_dbt_row.t_bankcorrcode,                                
                     t_corracc           = v_dsettacc_dbt_row.t_corracc,
                     t_code              = v_dsettacc_dbt_row.t_code
               where db.t_settaccid = v_dsettacc_dbt_row.t_settaccid;
               it_log.log(' Платежная инструкция обновлена в СПИ;'); 
             exception
               when others then
                 it_error.put_error_in_stack;
                 v_itt_spi_import_row.result_status := c_ERROR;
                 v_itt_spi_import_row.result_msg := ' Обновление СПИ завершено с ошибкой: '||sqlerrm;
                 it_log.log(' Обновление СПИ завершено с ошибкой;'||sqlerrm); 
                 it_error.clear_error_stack;
             end;                 
          else --новая запись v_upd_b = false            
            v_dsettacc_dbt_row.t_partyid       := v_partyid;    --t_partyid  ? заполнять значением идентификатора субъекта;                       
            v_dsettacc_dbt_row.t_fikind        := 1;            --t_fikind  ? Для новых записей заполняется константой ?1?         
            v_dsettacc_dbt_row.t_beneficiaryid := v_partyid;    --t_beneficiaryid    Для новых записей заполняется идентификатором субъекта                       
            v_dsettacc_dbt_row.t_codekind      := case cc.currency  --t_codekind  В рублях ? константой ?1? / В инвалюте ? константой ?6?
                                                    when 'RUB' then 1
                                                  else
                                                    6
                                                  end;
            v_dsettacc_dbt_row.t_code         := case cc.currency  --t_codekind  В рублях ? константой ?1? / В инвалюте ? константой ?6?
                                                    when 'RUB' then 1
                                                  else
                                                    6
                                                  end;  
            v_dsettacc_dbt_row.t_spi_ident :=  nvl(v_itt_spi_import_row.spi_id, v_itt_spi_import_row.si_internal);--t_spi_ident Для новых заполняется идентификатором СПИ                             
              
            --t_order Для новых записей заполняется инкрементом с шагом 5             
            select nvl(max(t_order),0) + 5 
              into v_dsettacc_dbt_row.t_order
              from dsettacc_dbt db          --Счет для расчетов с субъектами
             where db.t_partyid = v_partyid;--{идентификатор_субъекта}
             
            insert into dsettacc_dbt values v_dsettacc_dbt_row 
              returning t_settaccid into v_itt_spi_import_row.t_settaccid;
            
            v_itt_spi_import_row.t_partyid     := v_partyid;
            v_itt_spi_import_row.result_status := c_INSERT;
            v_itt_spi_import_row.result_msg    := v_itt_spi_import_row.result_msg||' Платежная инструкция добавлена в СПИ;';
            it_log.log(' Платежная инструкция добавлена в СПИ;');  
           end if;        
                  
        end if; --if v_rec.error_msg is null   

        v_result_text := v_result_text ||'Строка '''||v_itt_spi_import_row.excel_rownum||''': '|| v_itt_spi_import_row.result_msg ||chr(13)||chr(10);
        insert into itt_spi_import values v_itt_spi_import_row;
        exit when v_incorret_file_format; 
        
      exception
        when others then
          it_error.put_error_in_stack;         
                  
          v_result_text := v_result_text ||'Строка '''||v_itt_spi_import_row.excel_rownum||''': '||' '||Sqlerrm||chr(13)||chr(10);
          it_log.log(p_msg => 'Неизвестная ошибка'
                   , p_msg_type => it_log.C_MSG_TYPE__ERROR
                   , p_msg_clob => s_result_text
                     ); 
          it_error.clear_error_stack;
          v_itt_spi_import_row.result_msg := sqlerrm;
          v_itt_spi_import_row.result_status := c_ERROR;
          insert into itt_spi_import values v_itt_spi_import_row;          
      end;
    end loop;
    
   
    s_result_text := 'Количество обработанных строк '||v_data_row_count||':'||chr(13)||chr(10)||v_result_text; 

    if not v_find_b then
      raise_application_error(-20003,'Некорректный формат файла');
    end if;

    --убрать после отладки!!!!!!!
    --rollback to sp;
    -----------------------------
    
    it_log.log('end');
  exception
    when others then
      rollback to sp;
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Неизвестная ошибка'
               , p_msg_type => it_log.C_MSG_TYPE__ERROR
               , p_msg_clob => s_result_text
                 );
      it_error.clear_error_stack;  
      v_result_text := v_result_text||' Результат обработки отменен.'||Sqlerrm;
      s_result_text := v_result_text;  
  end Execute_Process;         

     --Получить длину (кол-во символов) текста результата
  function get_result_length return number 
  is
  begin
    return length(s_result_text)+1;
  end;
  
  --Получить текст результата
  function get_result_text return clob 
  is
  begin
    return s_result_text;
  end;
  
end;
/
