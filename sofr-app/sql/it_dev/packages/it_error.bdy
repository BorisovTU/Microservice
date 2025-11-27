create or replace package body it_error as

 /**************************************************************************************************\
  Пакет для сбора стека ошибки
  **************************************************************************************************
  Изменения:
  --------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                            Описание 
  ----------  ---------------  ---------------------------     -------------------------------------
  17.05.2022  Зыков М.В.       BIQ-11358                       Исправление ошибки  от 16.07.2021
  31.01.2022  Зотов Ю.Н.       BIQ-6664 CCBO-506               Создание  
 \**************************************************************************************************/
  
  c_stack_limit number := 100;
  
  s_last_sqlcode number; --текущий код ошибки
  s_last_error_msg varchar2(4000); --текущий полный текст ошибки
  
  s_backtrace_list dbms_sql.Varchar2_Table; --для накапливания полного стека ошибки (только трасса ошибки с номерами строк в пакетах)

  /*
  Предыдущие ошибки. Нужны для случая перехвата и одной ошибки и генерации другой, чтобы в стеке была видна и исходная ошибка.
  */
  s_last_sqlcode_list dbms_sql.Number_Table; --стек предыдущих кодов ошибок (не включает текущий s_last_sqlcode)
  s_last_error_msg_list dbms_sql.Varchar2_Table; --стек предыдущих полных текстов ошибок (не включает текущий s_last_error_msg)
  
  
  --удаляет из текста все строки, содержащие подстроку ORA-06512
  /*function remove_06512(p_text varchar2) return varchar2 is
    j1 number;
    j2 number;
    v_str varchar2(1000);
    v_result varchar2(32000);
  begin
    j1 := 1;
    j2 := instr(p_text,chr(10),j1); --в последней строке не будет перевода на новую строку, но в ней текст ошибки и она нам не нужна
    while j2 > 0 loop
      v_str := substr(p_text, j1, j2-j1+1); --выбираем очередную строку
      
      if instr(v_str,'ORA-06512') = 0 then --оставляем только строки, которые не содержат ORA-06512
        v_result := v_result || v_str;
      end if;  

      --переход к следующей строке
      j1 := j2+1;
      j2 := instr(p_text,chr(10),j1);
      --в последней строке не будет перевода на новую строку, но она нам нужна
      if j2 is null and j1<length(p_text) then
        j2 := length(p_text);
      end if;
      ----------------------------  
    end loop;  
    
    return v_result;
  end;*/    
  

  procedure put_error_in_stack as
    i integer;
    j1 integer;
    j2 integer;
    v_format_error_backtrace varchar2(32000);
    v_str varchar2(1000);
    v_trace varchar2(32000);
    v_line_position number;
    v_line number;
    k number;
    b_line_exists boolean;
  begin
    i:=s_backtrace_list.count();
    
    --если собранный стек превысил максимальную длинну, то удаляем первый элемент (принцип кольцевого буфера)
    if i > c_stack_limit then
      s_backtrace_list.delete(s_backtrace_list.first);
      if s_last_sqlcode_list.first is not null then
        s_last_sqlcode_list.delete(s_last_sqlcode_list.first);
      end if;
      if s_last_error_msg_list.first is not null then  
        s_last_error_msg_list.delete(s_last_error_msg_list.first);
      end if;  
    end if;  
    --------------------------------------------------------------------------------------------------------
    
    
    /*
    Парсим dbms_utility.format_error_backtrace и выбираем оттуда только стек ошибки.
    Но с Oracle 12 стек в bms_utility.format_error_backtrace может дублироваться, т.к. он сам его стал собирать, но, к сожалению, не на всю глубину.
    Дублирующиеся строки мы не будем собирать в наш стек.
    */  
    v_format_error_backtrace := dbms_utility.format_error_backtrace;
    j1 := 1;
    j2 := instr(v_format_error_backtrace,chr(10),j1); --в последней строке не будет перевода на новую строку, но в ней текст ошибки и она нам не нужна
    
    --проверяем, есть ли такая строка уже в стеке, если нет, то добавляем
    while j2 > 0 loop
      v_str := substr(v_format_error_backtrace, j1, j2-j1+1);

      v_line_position := instr(v_str,'line ');
      v_line :=rtrim(rtrim(substr(v_str,v_line_position+5),chr(10)),chr(13));

      --поиск строки с таким line в уже собранном стеке
      k:=s_backtrace_list.last;
      b_line_exists := false;
      while k is not null and not s_last_sqlcode_list.exists(k) loop --пока не закончилась коллекция или пока не пошла другая ошибка
        if instr(s_backtrace_list(k),'line '||v_line||chr(10)) > 0
        or instr(s_backtrace_list(k),'line '||v_line||chr(13)||chr(10)) > 0 then 
          b_line_exists := true; --эта строка трассы уже есть в нашем стеке - значит это дубликат и он нам не нужен
          exit;
        end if;  
        k:= s_backtrace_list.prior(k);
      end loop;
      -------------------------------------------------
          
      if not b_line_exists then --в нашей трассе не нужны дубликаты строк
        v_str := replace(replace(v_str,'ORA-06512: на  '),'ORA-06512: at '); --удаляем лишний неинформативный текст, чтобы уменьшить размер текста
        v_trace := v_trace || v_str;
      end if;  

      
      --переход к следующей строке текста
      j1 := j2+1;
      j2 := instr(v_format_error_backtrace,chr(10),j1); --в последней строке тоже есть перевод на новую строку
      -----------------------------------
    end loop;
    --------------------------------------------------------------------
    
    --сохраняем старую ошибку (например, когда подавляется одна ошибка, и вместо нее генерится другая)
    if s_last_sqlcode != sqlcode then
      s_last_sqlcode_list(i) := s_last_sqlcode; --старый код ошибки
      s_last_error_msg_list(i) := s_last_error_msg; --старый полный текст ошибки
    end if;
    -------------------------------------------------------------------------------------------------
    
    --текущий код ошибки  
    s_last_sqlcode := sqlcode;
    /*
    Cохраняем текущий полный текст ошибки, который при выводе добавится к трассе ошибки.
    Полный текст ошибки получаем из dbms_utility.format_error_stack путем удаления оттуда строк с трассой ошибки, т.к. трасса там плохая (хорошую трассу мы собираем алгоритмом выше)
    */
    --s_last_error_msg := rtrim(rtrim(remove_06512(dbms_utility.format_error_stack),chr(10)),chr(13)); --ошибкой ORA-06512 отмечены строки, относящиеся к трассе ошибки
    s_last_error_msg := rtrim(rtrim(sqlerrm,chr(10)),chr(13)); -- ORA-06512 может содержаться в собственно тексте ошибки, который кто-то передал. В таком случае не нужно это вычищать.
    

    --добавляем новый элемент в стек
    i:= i+1;
    s_backtrace_list(i) := v_trace;
    --------------------------------
  end;    

  procedure clear_error_stack as
  begin
    s_last_sqlcode := null;
    s_last_error_msg := null;
    
    s_backtrace_list.delete();
    
    s_last_sqlcode_list.delete();
    s_last_error_msg_list.delete();
  end;  

  function get_error_stack_clob(p_clear boolean default false) return clob is
    v_clob clob;
    i integer;
    v_str varchar2(1024);
    --v_str2 varchar2(1024);

  begin
    
    dbms_lob.createtemporary(lob_loc => v_clob,cache => true);
    dbms_lob.open(lob_loc => v_clob,open_mode => dbms_lob.lob_readwrite);
    
    
    i:=s_backtrace_list.first;
    while i is not null loop
      --если это уже трасса следующей ошибки (например, когда подавляется одна ошибка, и вместо нее генерится другая), то отделяем ее чертой при выводе 
      if s_last_sqlcode_list.exists(i-1) then
        v_str := rpad('-',length(s_backtrace_list(i)),'-')||chr(13)||chr(10);
        dbms_lob.writeappend(lob_loc => v_clob,
                             amount => length(v_str),
                             buffer => v_str); 
      end if;
      ------------------------------------------------------------------------------------------------------------------------------------------------ 
        
      /*16.07.2021 Есть почему-то иногда ошибка: ORA-06502: PL/SQL: numeric or value error*/
      --v_str2 := substr(s_backtrace_list(i),1,1024);
      if s_backtrace_list(i) is not null then
          dbms_lob.writeappend(lob_loc => v_clob,
                               amount => length(s_backtrace_list(i)),
                               buffer => s_backtrace_list(i)); 
       end if;                           

      --если трасса ошибки закончена, то нужно вывести ее полный текст, т.к. дальше пойдет трасса следующей ошибки                     
      if s_last_sqlcode_list.exists(i) then
        
        v_str := s_last_error_msg_list(i)||chr(13)||chr(10);
                    
        dbms_lob.writeappend(lob_loc => v_clob,
                             amount => length(v_str),
                             buffer => v_str); 

      end if;
      ------------------------------------------------------------------------------------------------------------ 

      i:= s_backtrace_list.next(i);
    end loop;    

    --выводим полный текст самой последней ошибки
    if s_last_error_msg is not null then
      dbms_lob.writeappend(lob_loc => v_clob,
                           amount => length(s_last_error_msg),
                           buffer => s_last_error_msg); 
    end if;
    ---------------------------------------------                           
    
    --если стек заполнен до лимита, то нужно предупредить, что он, возможно, не полный                       
    if s_backtrace_list.count = c_stack_limit then
      v_str := chr(13)||chr(10)||'Внимание! Достигнут предельный размер стека ошибок. Доступны только последние '||c_stack_limit||' ошибок.';
      dbms_lob.writeappend(lob_loc => v_clob,
                           amount => length(v_str),
                           buffer => v_str); 
    end if;
    ---------------------------------------------------------------------------------                        
    
    dbms_lob.close(lob_loc => v_clob);  
    
    if p_clear then
      clear_error_stack;
    end if;  

    return v_clob;
  exception
    when others then
      if dbms_lob.istemporary(v_clob) = 1 then
        if dbms_lob.isopen(v_clob) = 1 then
          dbms_lob.close(v_clob);
        end if;
        dbms_lob.freetemporary(v_clob);
      end if;
      raise;
  end;      


  function get_error_stack(p_clear boolean default false) return varchar2 is
    v_result varchar2(4000);
  begin
    
    v_result := substr(get_error_stack_clob(p_clear => p_clear),1,4000);

    return v_result;
  end;      
    

end;
/
