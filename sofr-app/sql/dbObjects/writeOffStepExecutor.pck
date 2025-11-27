create or replace package writeOffStepExecutor is
  /**
    @brief Opr_ExecuteStepByCodeAndNumberStep исполнить шаги операции  и закрыть ее. Макросы не исполняются. Эмулируется в SQL последний шаг 
    @param[in] p_documentid id операции
    @param[in] p_currentstep номер текущей операции
    @param[in] p_numberstep номер следующей операции
    @param[in] p_numberstep_close номер операции закрытия
    @return 0 - успешно добавлено, 1 - ошибка при выполнении
  */ 
  function Opr_ExecuteStepByCodeAndNumberStepSteps3(
    p_documentId in integer,
    p_currentstep in integer,
    p_numberstep in integer,
    p_numberstep_close in integer
    ) return integer;
end writeOffStepExecutor;
/
create or replace package body writeOffStepExecutor is
  l_sysdate date := NVL(
                   RSBSESSIONDATA.m_curdate,
                   -- sysdate временно
                   to_date('18102023','ddmmyyyy') --временно
                   );
  function Opr_ExecuteStepByCodeAndNumberStepSteps3(
    p_documentId in integer,
    p_currentstep in integer,
    p_numberstep in integer,
    p_numberstep_close in integer
    ) return integer is
    documentID varchar2(34) := lpad(to_char(p_documentId),34,'0');
    id_operation number(10);
    id_step_current integer;
    id_step integer;
    id_step_close integer;
    stat integer;
    BLOCKID integer := 203702;  -- Блок закрытия операции 2037 
  begin
    select step.t_id_step, oper.t_id_operation into id_step_current, id_operation
      from doprstep_dbt step, DOPROPER_DBT oper
      where step.t_id_operation = oper.t_id_operation
        and oper.t_documentid = documentID
        and step.t_blockid = BLOCKID
        and step.t_number_step = p_currentstep;
    select step.t_id_step, oper.t_id_operation into id_step, id_operation
      from doprstep_dbt step, DOPROPER_DBT oper
      where step.t_id_operation = oper.t_id_operation
        and oper.t_documentid = documentID
        and step.t_blockid = BLOCKID
        and step.t_number_step = p_numberstep;
    select step.t_id_step into id_step_close
      from doprstep_dbt step, DOPROPER_DBT oper
      where step.t_id_operation = oper.t_id_operation
        and oper.t_documentid = documentID
        and step.t_blockid = BLOCKID
        and step.t_number_step = p_numberstep_close;
    --return Rsbemoperation.Opr_ExecuteStep(id_operation,id_step);
    if (RSI_RsbOperation.ExecuteStepTrn(
        id_operation,
        id_step_current,
        chr(88),
        l_sysdate,
        1,
        0,
        0,
        l_sysdate) <> 0) then
      it_log.log('Ошибка в Opr_ExecuteStepByCodeAndNumberStepSteps3.RSI_RsbOperation.ExecuteStepTrn1');
      return 1;
    end if;
    if (RSI_RsbOperation.ExecuteStepTrn(
        id_operation,
        id_step,
        chr(88),
        l_sysdate,
        1,
        0,
        0,
        l_sysdate) <> 0) then
      it_log.log('Ошибка в Opr_ExecuteStepByCodeAndNumberStepSteps3.RSI_RsbOperation.ExecuteStepTrn2');
      return 1;
    end if;
    RSI_RsbOperation.SetOprStatusValueOnStep(
        stat,
        4607,
        id_operation,
        46073,
        2,
        id_step_close);
    if (stat<>0) then
      it_log.log('Ошибка в Opr_ExecuteStepByCodeAndNumberStepSteps3.RSI_RsbOperation.SetOprStatusValueOnStep1');
      return 1;
    end if;
    RSI_RsbOperation.SetOprStatusValueOnStep(
        stat,
        4607,
        id_operation,
        46075, --- почему тут используется вид сегмента статуса Возрат ДС?
        2,
        id_step_close);
    if (stat<>0) then
      it_log.log('Ошибка в Opr_ExecuteStepByCodeAndNumberStepSteps3.RSI_RsbOperation.SetOprStatusValueOnStep2');
      return 1;
    end if;
    if (RSI_RsbOperation.ExecuteStepTrn(
        id_operation,
        id_step_close,
        chr(88),
        l_sysdate,
        1,
        0,
        0,
        l_sysdate) <> 0) then
      it_log.log('Ошибка в Opr_ExecuteStepByCodeAndNumberStepSteps3.RSI_RsbOperation.ExecuteStepTrn3');
      return 1;
    end if;
    UPDATE dnptxop_dbt SET T_STATUS=2 WHERE T_ID=p_documentid;
    return 0;
  exception when others then
    it_log.log('Ошибка в Opr_ExecuteStepByCodeAndNumberStepSteps3');
    return 1;
  end;

end writeOffStepExecutor;
/
