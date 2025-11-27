begin
  for rec in (
        -- заявления
        with petition as (
        select *
            from dnptxop_dbt ds
            where ds.t_operdate >= to_date('01012023','ddmmyyyy') -- с 1 января 2023
            and ds.t_operdate < sysdate -- по текущий операционный день(??)
            and ds.t_status = 3 -- статус Проверено
            and ds.t_dockind = 4644
            ),
        -- обработка заявлений по клиентам 
        processes as (
          select nx.*, 
                 pt.t_tax t_sum_petition, -- сумма заявления
                 pt.t_id t_id_petition, -- id заявления
                 pt.t_code t_code_petition -- код заявления
          from dnptxop_dbt nx, petition pt 
          where nx.t_client = pt.t_client
          and nx.t_operdate >= to_date('01012023','ddmmyyyy') -- с 1 января 2023
          and nx.t_operdate < sysdate -- по текущий операционный день (??)
          and nx.t_dockind = 4642
          and nx.t_status = 2 -- закрытая обработка
          )
        -- обработка заявлений с проводками и с суммой, равной сумме заявления
        select p.* from processes p where 
          exists (
            select trn.t_AccTrnID from doproper_dbt childop, doprdocs_dbt oprdocs, 
              doproper_dbt parentop, dacctrn_dbt trn 
           where parentop.t_DocKind=4642 AND parentop.t_DocumentID=LPAD(p.t_id, 34, chr(48)) 
         AND childop.t_Parent_ID_Operation=parentop.t_ID_Operation 
         AND oprdocs.t_ID_Operation=childop.t_ID_Operation 
         AND oprdocs.t_DocKind=1 AND trn.t_AccTrnID=oprdocs.t_AccTrnID 
         AND trn.t_Chapter=1 
         AND trn.t_Account_Payer LIKE('603%')
         AND p.t_sum_petition = trn.t_sum_natcur
         AND substr(trn.t_ground,1,3) = 'Воз'
         )
      ) loop
        --dbms_output.put_line('Обновление статуса с Проверено на Закрыто заявления с id '||rec.t_id_petition||' и с кодом '||rec.t_code_petition);
        it_log.log('Обновление статуса с Проверено на Закрыто заявления с id '||rec.t_id_petition||' и с кодом '||rec.t_code_petition);
        update dnptxop_dbt pet 
          set pet.t_status = 5 -- статус закрыто
          where 
            pet.t_operdate >= to_date('01012023','ddmmyyyy') -- с 1 января 2023
            and pet.t_operdate < sysdate -- по текущий операционный день (??)
            and pet.t_status = 3 -- статус Проверено
            and pet.t_dockind = 4644
            and pet.t_id = rec.t_id_petition;
  end loop;
  --dbms_output.put_line('Завершен скрипт 112_DEF-74909_update_nptxop.sql');
  it_log.log('Завершен скрипт 112_DEF-74909_update_nptxop.sql');
end;
