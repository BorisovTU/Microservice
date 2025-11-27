CREATE OR REPLACE PACKAGE BODY RSI_SUMCONFEXP IS

 /**

  -- Author  : Nikonorov Evgeny
  -- Purpose : Пакет подготовки данных для отчета "Суммы подтвержденных расходов"

# changelog
 |date       |autor          |tasks                                           |note
 |-----------|---------------|------------------------------------------------|-------------------------------------------------------------
 |26.11.2025 |Велигжанин А.В.|DEF-112611                                      |GenSumConfirmChunks (), Генерация заданий для отчета
 |20.11.2025 |Велигжанин А.В.|DEF-112161                                      |Доработка для пустых дат.
 |17.11.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1350                      |g_DepoProcessed, Флаг обработки сделки зачисления ДЕПО 
 |           |               |                                                |во избежание дублирования данных
 |14.11.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-2262                      |Доработка GetConvStatus0(), если лот конвертации относится к сделке
 |           |               |                                                |перевода между счетами, нужно его запоминать,
 |           |               |                                                |чтобы затем верно обработать
 |12.11.2025 |Велигжанин А.В.|DEF-110778                                      |Доработка CallExecuteCodeInQManager():
 |           |               |                                                |1) процессам передается флаг отладки
 |07.11.2025 |Велигжанин А.В.|DEF-109328                                      |CreateRepData (..., p_OnDate)
 |07.11.2025 |Велигжанин А.В.|DEF-109307                                      |GenFiidChunks(), генерация заданий по всем клиентам с полученным ФИ
 |30.10.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1703                      |InitParallelCalc(), ClearParallelCalc(), DeleteParallelCalc()
 |29.10.2025 |Велигжанин А.В.|DEF-107734                                      |Доработка ProcessConv (), обработка лота конвертации для сделки зачисления ДЕПО
 |           |               |                                                |с заменой на сделки "зачисления НДФЛ" возвращена обратно
 |27.10.2025 |Велигжанин А.В.|AVT_BOSS-1767_AVT_BOSS-1309                     |Перенос задач по доработке в release-122.0
 |27.10.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1356                      |ProcessNdfl (..., p_SumNdfl), учет остатка суммы 
 |           |               |                                                |сделки "зачисления ДЕПО"
 |27.10.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1757                      |Учет нескольких конвертаций, доработка ProcessDepo()
 |20.10.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1309                      |Учет нескольких конвертаций
 |17.10.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1350                      |SetDebugFlag(), Установка флага отладки
 |16.10.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1356                      |Доработка ProcessNdfl (), если сделки "зачисления НДФЛ" не 
 |           |               |                                                |удалось определить по ID сделки "зачисления ДЕПО", 
 |           |               |                                                |нужно на ID не смотреть
 |16.10.2025 |Велигжанин А.В.|AVT_BOSS-573_AVT_BOSS-1354                      |Доработка ProcessNdfl (), сделки "зачисления НДФЛ" выбираются
 |           |               |                                                |по валюте сделки "зачисления ДЕПО"
 |06.10.2025 |Велигжанин А.В.|DEF-103125                                      |Доработка ProcessNdfl (), поправлено определение суммы 
 |           |               |                                                |для граничной сделки
 |10.09.2025 |Велигжанин А.В.|DEF-101302                                      |Доработка ProcessNdfl () для выборки сделок "зачисления НДФЛ"
 |           |               |                                                |по списку валют
 |10.09.2025 |Велигжанин А.В.|DEF-101246                                      |Доработка выборки сделок "зачисления НДФЛ" с учетом суммы и конвертации
 |08.09.2025 |Зыков М.В.     |DEF-100823                                      |Существенное замедление выполнения RSI_SUMCONFEXP.CreateSumConfirmExpRepData
 |25.08.2025 |Велигжанин А.В.|DEF-98100                                       |ProcessDepo(), замена сделок "Зачисления ДЕПО"
 |           |               |                                                |соответствующими сделками "Зачисления НДФЛ"
 |10.06.2025 |Велигжанин А.В.|DEF-92673                                       |Поправлен расчет x_DealSumPayFI
 |10.05.2025 |Велигжанин А.В.|DEF-78108                                       |GetPrev(), убран из процесса из-за замедления
 |03.05.2025 |Велигжанин А.В.|DEF-89639                                       |Доработка для GetOutDealID()
 |29.04.2025 |Велигжанин А.В.|DEF-88733                                       |Заполнение DMASEXEC_DBT даже для одного договора,
 |           |               |                                                |так как через него теперь работает печать через FuncObj
 |15.04.2025 |Велигжанин А.В.|DEF-88167                                       |Доработка ProcessIsSkip(), нужно изменять t_IsSkip
 |           |               |                                                |для сделок зачисления ДЕПО только по отдельным бумагам,
 |           |               |                                                |по которым есть сделки зачисления НДФЛ
 |07.04.2025 |Велигжанин А.В.|DEF-84364                                       |Поправка: ProcessOneBuyDeal(), расчет Cost и TotalCost
 |04.04.2025 |Велигжанин А.В.|DEF-84364                                       |Поправка: цены 'Зачислений НДФЛ' нужно также пересчитывать,
 |           |               |                                                |если по ним была конвертация
 |03.04.2025 |Велигжанин А.В.|DEF-84364                                       |Поправка: UpdateNdflConv() нужно вызывать только для сделок 
 |           |               |                                                |'Зачислений ДЕПО', если по ним была конвертация
 |02.04.2025 |Велигжанин А.В.|DEF-84364                                       |GetNdflTicks(), запоминается валюта во избежании дублей
 |           |               |                                                |UpdateNdflConv(), обновление коэффициентов для 'Зачислений НДФЛ',
 |           |               |                                                |если была конвертация
 |10.02.2025 |Велигжанин А.В.|DEF-81824                                       |ProcessIsSkip(), пропуск сделок зачисления ДЕПО по признаку t_IsSkip,
 |           |               |                                                |а не по признаку t_IsDepo
 |22.01.2025 |Велигжанин А.В.|DEF-80463                                       |Доработка процедуры параллельной обработки данных
 |           |               |                                                |(используется itt_parallel_exec)
 |13.01.2025 |Велигжанин А.В.|DEF-79765                                       |Исправлена ошибка в CreateRepDataByContr_1()
 |13.01.2025 |Велигжанин А.В.|DEF-79765                                       |Доработка для режима 'Переведенные другому брокеру'
 |           |               |                                                |для не заданной даты, запрос изменен для выборки всех переводов
 |19.12.2024 |Велигжанин А.В.|DEF-78063                                       |Доработка для режима 'Переведенные другому брокеру'
 |           |               |                                                |для не заданной даты (используем дату опер.дня)
 |11.12.2024 |Велигжанин А.В.|DEF-77821                                       |Доработка для конвертаций режима 'Переведенные другому брокеру'
 |           |               |                                                |Замена зачислений ДЕПО зачислениями НДФЛ
 |28.11.2024 |Велигжанин А.В.|DEF-77256                                       |Доработка для режима 'Переведенные другому брокеру'
 |           |               |                                                |Вызов CreateRepData() после сбора данных FIID
 |28.11.2024 |Велигжанин А.В.|DEF-73150                                       |Изменено условие отбора сделок продажи в режиме
 |           |               |                                                |'Переведенные другому брокеру' (сразу определяется FIID)
 |20.11.2024 |Велигжанин А.В.|DEF-73333                                       |Рефакторинг CreateRepData(), можно (и нужно) запускать 
 |           |               |                                                |не на каждой итерации сбора данных по FIID
 |18.11.2024 |Велигжанин А.В.|DEF-73119                                       |GetSubstInfo(), инфа о сделке замещения
 |13.11.2024 |Велигжанин А.В.|DEF-73272                                       |GetNdflTicks(), сбор данных о сделках 'Зачисления НДФЛ' 
 |           |               |                                                |(инфа о которых отсутствует в лотах)
 |26.08.2024 |Велигжанин А.В.|BOSS-2935                                       |Для режима 'ЦБ на дату' данные подготавливаются через
 |           |               |                                                |таблицу лотов dpmwrtsum_dbt (если дата отчета > макс. даты изменения лотов)
 |           |               |                                                |или через архив лотов
 |09.08.2024 |Велигжанин А.В.|DEF-69222                                       |Изменен алгоритм для режима 'Переведенные другому брокеру'
 |           |Никоноров Е.   |                                                |Создан

  */

  PARALLEL_LEVEL CONSTANT NUMBER(5) := 10; --количество потоков
  g_ParallelLimit NUMBER := 8;  -- макс. число параллельных потоков

  x_ViewedSubst t_Nodes;
  x_ViewedDepo t_Nodes;
  x_ViewedNdfl t_Nodes;
  x_RegProcessConv BOOLEAN := TRUE; -- Rsb_Common.GetRegBoolValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\КОНВЕР_В_СПРАВКЕ_ГОССЛ');
  g_DebugFlag NUMBER := 0;  -- флаг ошибки установлен?
  g_NullDate DATE := TO_DATE('01.01.0001','DD.MM.YYYY');

  -- AVT_BOSS-573_AVT_BOSS-1309, учет нескольких конвертаций
  g_Numerator NUMBER := 1;
  g_Denominator NUMBER := 1;
  g_Cur NUMBER;
  g_DepoProcessed NUMBER; -- AVT_BOSS-1350 Флаг обработки сделки зачисления ДЕПО во избежание дублирования данных

  TYPE scsumconfexp_t IS TABLE OF dscsumconfexp_dbt%ROWTYPE;

  /**
   @brief    Функция для журналирования времени выполнения.
  */
  FUNCTION ElapsedTime ( p_time IN pls_integer ) return varchar2 
  IS
  BEGIN
    RETURN to_char((dbms_utility.get_time - p_time) / 100, 'fm9999999990D00');
  END ElapsedTime;

  /** 
   @brief    Возвращает сумму комиссии по данным DlSum
  */
  FUNCTION GetDlSumCommiss( p_DocKind IN NUMBER, p_DocID IN NUMBER ) 
    RETURN NUMBER deterministic
  IS
    x_Sum NUMBER := 0;
  BEGIN
    SELECT NVL(SUM( RSI_RSB_FIInstr.ConvSum(t_Sum, t_Currency, RSI_RSB_FIInstr.NATCUR, t_Date )), 0)
      INTO x_Sum
      FROM ddlsum_dbt
     WHERE t_DocKind = p_DocKind
       AND t_DocID = p_DocID
       AND t_Kind = RSB_SECUR.DLSUM_OUTLAYWRTTAX
       AND ROWNUM = 1;
    RETURN x_Sum;
  EXCEPTION WHEN OTHERS THEN 
    RETURN 0;
  END;

  /** 
   @brief    Возвращает сумму комиссии по данным ddlcomis_dbt
  */
  FUNCTION GetDlCommiss( p_DocKind IN NUMBER, p_DocID IN NUMBER ) 
    RETURN NUMBER deterministic
  IS
    x_Sum NUMBER := 0;
  BEGIN
    SELECT NVL(SUM( RSI_RSB_FIInstr.ConvSum( dlcomis.t_Sum, comis.t_FIID_Comm, RSI_RSB_FIInstr.NATCUR, GREATEST(dlcomis.t_PlanPayDate, dlcomis.t_FactPayDate) )), 0)
      INTO x_Sum
      FROM ddlcomis_dbt dlcomis, dsfcomiss_dbt comis
     WHERE dlcomis.t_DocKind   = p_DocKind
       AND dlcomis.t_DocID     = p_DocID
       AND dlcomis.t_FeeType   = comis.t_FeeType
       AND dlcomis.t_ComNumber = comis.t_Number;
    RETURN x_Sum;
  EXCEPTION WHEN OTHERS THEN 
    RETURN 0;
  END;

  /** 
   @brief    Возвращает сумму комиссии по одной сделке
  */
  FUNCTION GetDealCommiss( p_DocKind IN NUMBER, p_DocID IN NUMBER ) 
    RETURN NUMBER deterministic
  IS
    x_Sum NUMBER := 0;
  BEGIN
    IF( p_DocKind <> RSB_SECUR.DL_AVRWRT ) THEN
      x_Sum := GetDlCommiss( p_DocKind, p_DocID);
    ELSE
      x_Sum := GetDlSumCommiss( p_DocKind, p_DocID);
      IF(x_Sum = 0) THEN
        x_Sum := GetDlCommiss( p_DocKind, p_DocID);
      END IF;
    END IF;
    RETURN x_Sum;
  END;

  /** 
   @brief    Возвращает ID сделки замещения, если она есть, иначе -1
  */
  FUNCTION GetSubstDeal( p_DealID IN NUMBER ) 
    RETURN NUMBER deterministic
  IS
    x_Subst NUMBER := -1;
  BEGIN
     SELECT
       case when nt.t_Text is not null then rsb_struct.getInt(nt.t_Text) else -1 end INTO x_Subst
     FROM ddl_tick_dbt t 
     LEFT JOIN dobjatcor_dbt ac ON ( ac.t_ObjectType = t.t_bofficekind AND ac.t_GroupID = 55 AND ac.t_Object = lpad(t.t_dealid,34,'0') )
     LEFT JOIN dnotetext_dbt nt ON ( nt.t_objecttype = t.t_bofficekind and nt.t_documentid = lpad(t.t_dealid,34,'0') and nt.t_notekind = 38)
     WHERE t.t_dealid = p_DealID
     ;
    RETURN x_Subst;
  EXCEPTION
    WHEN OTHERS THEN 
    RETURN x_Subst;
  END GetSubstDeal;

  /** 
   @brief    Возвращает информацию о сделке замещения: валюту, клиента, дату сделки
  */
  PROCEDURE GetSubstInfo( p_DealID IN NUMBER, p_Fiid OUT NUMBER, p_PartyID OUT NUMBER, p_DealDate OUT DATE ) 
  AS
  BEGIN
    p_Fiid := -1;
    p_PartyID := -1;
    p_DealDate := sysdate;
    SELECT r.t_pfi, r.t_clientid, r.t_dealdate 
      INTO p_FIID, p_PartyID, p_DealDate
      FROM ddl_tick_dbt r WHERE r.t_dealid = p_DealID;
  EXCEPTION
    WHEN OTHERS THEN 
      NULL;
  END GetSubstInfo;

  /** 
   @brief    Возвращает информацию о лоте конвертации: валюту, клиента, коэффициенты конвертации
  */
  PROCEDURE GetConvInfo( 
    p_SumID IN NUMBER, p_Fiid OUT NUMBER, p_PartyID OUT NUMBER, p_Numerator OUT NUMBER, p_Denominator OUT NUMBER, p_SumPrecision OUT NUMBER
  ) 
  AS
  BEGIN
    p_Fiid := -1;
    p_PartyID := -1;
    p_Numerator := 1;
    p_Denominator := 1;
    p_SumPrecision := 6;
    SELECT 
      bt.t_pfi, bt.t_clientid 
      , it_xml.char_to_number(coef.t_numerator) AS t_numerator		-- DEF-101246 исправление преобразования числа 
      , it_xml.char_to_number(coef.t_denominator) AS t_denominator	-- DEF-101246 исправление преобразования числа 
      , NVL(coef.t_sumprecision,6) AS t_sumprecision
    INTO p_FIID, p_PartyID, p_Numerator, p_Denominator, p_SumPrecision
    FROM dpmwrtsum_dbt cl 
    INNER JOIN dscdlfi_dbt coef on (coef.t_dealkind = cl.t_dockind AND coef.t_dealid = cl.t_docid) -- коэфициенты конвертации
    INNER JOIN dpmwrtlnk_dbt lnk on (lnk.t_saleid = cl.t_parent)  -- связка между лотом-конвертации и лотом-покупки
    INNER JOIN dpmwrtsum_dbt bl ON (bl.t_sumid = lnk.t_buyid)     -- лот-покупки
    INNER JOIN ddl_tick_dbt bt ON (bt.t_dealid = bl.t_DealID) -- ценовые условия покупки
    WHERE cl.t_sumid = p_SumID AND ROWNUM = 1
    ;
  EXCEPTION
    WHEN OTHERS THEN 
      NULL;
  END GetConvInfo;

  /** 
   @brief    Возвращает сумму комиссий для цепочки сделок
  */
  FUNCTION GetSubstSum( p_DealID IN NUMBER, p_SubstDate OUT DATE ) 
    RETURN NUMBER deterministic
  IS
    x_Sum NUMBER := 0;
    x_FIID NUMBER;
    x_PartyID NUMBER;
    x_DocKind NUMBER;
    x_DealDate DATE;
    x_SubstSum NUMBER;
  BEGIN
    GetSubstInfo(p_DealID, x_FIID, x_PartyID, x_DealDate);
    IF(x_FIID = -1) THEN
      RETURN 0;
    END IF;
    -- Проход по истории сделок и вычисление коммисий по ним (сделки перебираются в обратном порядке)
    FOR i IN (
      SELECT DISTINCT t_dealID, t_dealDate FROM (
        SELECT v.t_dealID, v.t_dealDate FROM v_scwrthistex v 
        WHERE v.t_fiid = x_FIID AND v.t_party = x_PartyID AND v.t_dealdate <= x_DealDate )
      ORDER BY t_dealDate DESC
    ) LOOP
      SELECT r.t_bofficekind INTO x_DocKind FROM ddl_tick_dbt r WHERE r.t_dealid = i.t_dealID;
      x_SubstSum := GetCommiss( x_DocKind, i.t_dealID, p_SubstDate );
      x_Sum := x_Sum + x_SubstSum;
      p_SubstDate := i.t_dealDate;
    END LOOP;
    -- DEF-73119 Определение даты первоначального приобретения
    SELECT NVL(min(least_date), p_SubstDate)  
      INTO p_SubstDate
      FROM (
      SELECT least(t.t_dealdate, sm.t_date) AS least_date 
        FROM ddl_tick_dbt t 
        LEFT JOIN ddlsum_dbt sm ON sm.t_dockind = t.t_bofficekind and sm.t_docid = t.t_dealid and sm.t_kind = 1220
        WHERE t.t_clientid = x_PartyID  and t.t_pfi = x_FIID)
      ;
    IF(x_Sum <> 0) THEN
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'SubstSum('||p_DealID||'): '||x_Sum||', p_SubstDate='||p_SubstDate ) ;
      END IF;
    END IF;
    RETURN x_Sum;
  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'x_DocKind: '||SQLERRM ) ;
      END IF;
      RETURN 0;
  END GetSubstSum;

  /** 
   @brief    Возвращает сумму комиссии.
             BOSS-2935, для сделки замещения сумма комиссии должна включать также сумму комиссий по цепочке сделок
  */
  FUNCTION GetCommiss( p_DocKind IN NUMBER, p_DocID IN NUMBER, p_SubstDate OUT DATE ) 
    RETURN NUMBER deterministic
  IS
    x_Sum NUMBER := 0;
    x_Subst NUMBER;
    x_SubstSum NUMBER;
  BEGIN
    x_Sum := GetDealCommiss( p_DocKind, p_DocID );
    x_ViewedSubst(p_DocID) := 1;  -- запоминаем, что этот узел уже был просмотрен в процессе определения комиссии
    IF(x_RegProcessConv) THEN
      x_Subst := GetSubstDeal( p_DocID );
      IF((x_Subst <> -1) AND (not x_ViewedSubst.exists( x_Subst ))) THEN
        x_ViewedSubst(x_Subst) := 1; -- помечаем и этот узел, как просмотренный
        x_SubstSum := GetSubstSum( x_Subst, p_SubstDate );
        IF(g_DebugFlag = 1) THEN
          it_log.log( p_msg => 'Сделка замещения '||p_DocID||', x_SubstSum: '||x_SubstSum ) ;
        END IF;
        x_Sum := x_Sum + x_SubstSum;
      END IF;
    END IF;
    RETURN x_Sum;
  END GetCommiss;

  PROCEDURE CreateDealDataForRest(p_GUID IN VARCHAR2,
                                  p_DlContrID IN NUMBER,                                                                           
                                  p_OnDate IN DATE,
                                  p_FIID IN NUMBER,
                                  p_Rest IN NUMBER,
                                  p_OutDate IN DATE
                                 )
  AS
    v_PartyID NUMBER;
    v_Sort NUMBER := 0;
    v_K    NUMBER := 0;
    v_Rest NUMBER := 0;
    
    v_prevAmount        NUMBER := 0;
    v_prevDealSum_PayFI NUMBER := 0;
    v_prevNKD_PayFI     NUMBER := 0;
    v_prevDealSum_Nat   NUMBER := 0;
    v_prevSumComiss     NUMBER := 0;
    v_totalCost         NUMBER := 0;
    x_SubstDate DATE;

    v_wasCalc BOOLEAN := FALSE;

    scsumconfexp_ins scsumconfexp_t := scsumconfexp_t();

    v_scsumconfexp   DSCSUMCONFEXP_DBT%rowtype;
    x_clob clob;
  BEGIN

    x_clob := 
        to_clob('p_GUID: ['||p_GUID||']'||chr(13)||chr(10))
        || to_clob('p_DlContrID: ['||to_char(p_DlContrID)||']'||chr(13)||chr(10))
        || to_clob('p_OnDate: ['||to_char(p_OnDate, 'yyyy-mm-dd')||']'||chr(13)||chr(10))
        || to_clob('p_FIID: ['||to_char(p_FIID)||']'||chr(13)||chr(10))
        || to_clob('p_Rest: ['||to_char(p_Rest)||']'||chr(13)||chr(10))
    ;
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'Start', p_msg_clob => x_clob ) ;
    END IF;

    IF p_DlContrID <= 0 THEN 
      RETURN; 
    END IF;

    SELECT sf.t_PartyID INTO v_PartyID
      FROM ddlcontr_dbt dlcontr, dsfcontr_dbt sf
     WHERE dlcontr.t_DlContrID = p_DlContrID
       AND sf.t_ID = dlcontr.t_SfContrID;

    v_Rest := p_Rest;

    FOR one_tick IN (SELECT q_tk.t_PFI, q_tk.t_BOfficeKind, q_tk.t_DealID,
                            leg.t_Principal, leg.t_Maturity, leg.t_Expiry, 
                            (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT 
                                  THEN NVL((SELECT t_Currency 
                                             FROM ddlsum_dbt 
                                           WHERE t_DocKind = RSB_SECUR.DL_AVRWRT 
                                             AND t_DocID = q_tk.t_DealID
                                             AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
                                       , 0)
                                  ELSE leg.t_CFI END) as t_CFI, 
                            (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT 
                                  THEN NVL((SELECT t_Sum 
                                             FROM ddlsum_dbt 
                                           WHERE t_DocKind = RSB_SECUR.DL_AVRWRT 
                                             AND t_DocID = q_tk.t_DealID
                                             AND t_Kind = RSB_SECUR.DLSUM_PRICEWRTTAX)
                                       , 0)
                                  ELSE leg.t_Price END) as t_Price, 
                            (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT 
                                  THEN NVL((SELECT t_Sum 
                                             FROM ddlsum_dbt 
                                           WHERE t_DocKind = RSB_SECUR.DL_AVRWRT 
                                             AND t_DocID = q_tk.t_DealID
                                             AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
                                       , 0)
                                  ELSE leg.t_Cost END) as t_Cost, 
                            (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT 
                                  THEN NVL((SELECT t_Sum 
                                             FROM ddlsum_dbt 
                                           WHERE t_DocKind = RSB_SECUR.DL_AVRWRT 
                                             AND t_DocID = q_tk.t_DealID
                                             AND t_Kind = RSB_SECUR.DLSUM_NKDWRTTAX)
                                       , 0)
                                  ELSE leg.t_NKD END) as t_NKD, 
                            leg.t_TotalCost, 
                            leg.t_MaturityIsPrincipal, 
                            fin.t_FaceValueFI,
                            (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT
                                  THEN CASE WHEN q_tk.t_flag3=chr(88) AND q_tk.t_taxownbegdate != g_NullDate 
                                            THEN q_tk.t_taxownbegdate ELSE
                                         NVL((SELECT t_Date
                                               FROM ddlsum_dbt
                                             WHERE t_DocKind = RSB_SECUR.DL_AVRWRT
                                               AND t_DocID = q_tk.t_DealID
                                               AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
                                         , leg.t_Start)
                                       END
                                  ELSE q_tk.t_DealDate END) as t_DealDate,
                            (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT 
                                  THEN NVL((SELECT t_Date
                                             FROM ddlsum_dbt 
                                           WHERE t_DocKind = RSB_SECUR.DL_AVRWRT 
                                             AND t_DocID = q_tk.t_DealID
                                             AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
                                       , leg.t_Start) 
                                  WHEN leg.t_MaturityIsPrincipal = 'X' THEN leg.t_Maturity 
                                  ELSE leg.t_Expiry END) as t_SettlDate,
                            (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT
                                  THEN NVL((SELECT t_Date
                                               FROM ddlsum_dbt
                                             WHERE t_DocKind = RSB_SECUR.DL_AVRWRT
                                               AND t_DocID = q_tk.t_DealID
                                               AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
                                         , leg.t_Start)
                                  ELSE q_tk.t_DealDate END) as t_DealDate_Rate
                       FROM (SELECT tk.*
                               FROM (SELECT t_Kind_Operation 
                                       FROM doprkoper_dbt 
                                      WHERE t_DocKind = RSB_SECUR.DL_SECURITYDOC
                                        AND Rsb_Secur.IsBuy(rsb_secur.get_OperationGroup(t_SysTypes)) = 1
                                        AND Rsb_Secur.IsTwoPart(rsb_secur.get_OperationGroup(t_SysTypes)) = 0
                                    ) opr, ddl_tick_dbt tk, ddlcontrmp_dbt mp
                              WHERE tk.t_BOfficeKind = RSB_SECUR.DL_SECURITYDOC
                                AND tk.t_DealType = opr.t_Kind_Operation
                                AND tk.t_ClientID = v_PartyID
                                AND tk.t_DealDate <= p_OnDate
                                AND tk.t_PFI = p_FIID
                                AND tk.t_DealStatus = 20 --Закрыта
                                AND mp.t_SfContrID = tk.t_ClientContrID
                                AND mp.t_DlContrID = p_DlContrID
                             UNION
                             SELECT /*+ index(tk DDL_TICK_DBT_IDX_U2)*/ tk.*
                               FROM (SELECT t_Kind_Operation 
                                            FROM doprkoper_dbt 
                                           WHERE t_DocKind = RSB_SECUR.DL_SECURITYDOC
                                             AND Rsb_Secur.IsSale(rsb_secur.get_OperationGroup(t_SysTypes)) = 1
                                             AND Rsb_Secur.IsTwoPart(rsb_secur.get_OperationGroup(t_SysTypes)) = 0
                                    ) opr, ddl_tick_dbt tk, ddlcontrmp_dbt mp
                              WHERE tk.t_BOfficeKind = RSB_SECUR.DL_SECURITYDOC
                                AND tk.t_DealType = opr.t_Kind_Operation
                                AND tk.t_IsPartyClient = 'X'
                                AND tk.t_PartyID = v_PartyID
                                AND tk.t_DealDate <= p_OnDate
                                AND tk.t_PFI = p_FIID
                                AND tk.t_DealStatus = 20 --Закрыта
                                AND mp.t_SfContrID = tk.t_PartyContrID
                                AND mp.t_DlContrID = p_DlContrID
                              UNION
                             SELECT tk.*
                               FROM (SELECT t_Kind_Operation 
                                            FROM doprkoper_dbt 
                                           WHERE t_DocKind = RSB_SECUR.DL_AVRWRT
                                             AND Rsb_Secur.IsAvrWrtIn(rsb_secur.get_OperationGroup(t_SysTypes)) = 1
                                    ) opr, ddl_tick_dbt tk, ddlcontrmp_dbt mp
                              WHERE tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT
                                AND tk.t_DealType = opr.t_Kind_Operation
                                AND tk.t_ClientID = v_PartyID
                                AND tk.t_DealDate <= p_OnDate
                                AND tk.t_Flag3 = 'X'
                                AND tk.t_PFI = p_FIID
                                AND tk.t_DealStatus = 20 --Закрыта
                                AND mp.t_SfContrID = tk.t_ClientContrID
                                AND mp.t_DlContrID = p_DlContrID
                            ) q_tk, ddl_leg_dbt leg, dfininstr_dbt fin
                      WHERE leg.t_DealID = q_tk.t_DealID
                        AND leg.t_LegKind = 0
                        AND leg.t_LegID = 0
                        AND fin.t_FIID = q_tk.t_PFI
                     ORDER BY (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN leg.t_Start ELSE q_tk.t_DealDate END) DESC, 
                              (CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN leg.t_SupplyTime ELSE q_tk.t_DealDate END) DESC, 
                              q_tk.t_DealID DESC
                    )
    LOOP

      v_Sort := v_Sort + 1;

      v_scsumconfexp.T_GUID           := p_GUID;
      v_scsumconfexp.T_CLIENTID       := v_PartyID;
      v_scsumconfexp.T_DLCONTRID      := p_DlContrID;
      v_scsumconfexp.T_FIID           := one_tick.t_PFI;
      v_scsumconfexp.T_SORT           := v_Sort;
      v_scsumconfexp.T_DEALID         := one_tick.t_DealID;
      v_scsumconfexp.T_DEALDATE       := one_tick.t_DealDate;

      v_scsumconfexp.T_DEALDATE_RATE  := 0.0;
      IF one_tick.t_CFI = RSI_RSB_FIInstr.NATCUR THEN
        v_scsumconfexp.T_DEALDATE_RATE  := 1.0;
      ELSE
        v_scsumconfexp.T_DEALDATE_RATE := RSI_RSB_FIINSTR.ConvSumType(1.0, one_tick.t_CFI, RSI_RSB_FIInstr.NATCUR, 7, one_tick.t_DealDate_Rate);
      END IF;

      v_scsumconfexp.T_SETTLDATE      := one_tick.t_SettlDate;

      v_scsumconfexp.T_SETTLDATE_RATE  := 0.0;
      IF one_tick.t_CFI = RSI_RSB_FIInstr.NATCUR THEN
        v_scsumconfexp.T_SETTLDATE_RATE  := 1.0;
      ELSE
        v_scsumconfexp.T_SETTLDATE_RATE := RSI_RSB_FIINSTR.ConvSumType(1.0, one_tick.t_CFI, RSI_RSB_FIInstr.NATCUR, 7, v_scsumconfexp.T_SETTLDATE);
      END IF;

      v_scsumconfexp.T_AMOUNT         := LEAST(v_Rest, one_tick.t_Principal);

      v_K := v_scsumconfexp.T_AMOUNT / one_tick.t_Principal;


      v_scsumconfexp.T_CFI            := one_tick.t_CFI;

      v_scsumconfexp.T_FACEVALUE      := RSI_RSB_FIInstr.FI_GetNominalOnDate(one_tick.t_PFI, one_tick.t_DealDate);

      v_scsumconfexp.T_PRICE          := one_tick.t_Price;

      IF one_tick.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN
        v_totalCost := one_tick.t_Cost + one_tick.t_NKD;
      ELSE
        v_totalCost := one_tick.t_TotalCost;
      END IF;

      v_wasCalc := FALSE;
      IF( p_OutDate > g_NullDate AND v_scsumconfexp.T_AMOUNT <> one_tick.t_Principal ) THEN
        --Если сделка попадает частично, то проверим (для режима Переведенные другому брокеру), если ли уже эта сделка за другие даты, чтобы правильно сделать округление
        SELECT NVL(SUM(T_AMOUNT), 0),
               NVL(SUM(T_DEALSUM_PAYFI), 0),
               NVL(SUM(T_NKD_PAYFI), 0),    
               NVL(SUM(T_DEALSUM_NAT), 0),  
               NVL(SUM(T_SUMCOMISS), 0)    
          INTO v_prevAmount, v_prevDealSum_PayFI, v_prevNKD_PayFI, v_prevDealSum_Nat, v_prevSumComiss
          FROM dscsumconfexp_dbt
         WHERE t_GUID = p_GUID
           AND t_ClientID = v_PartyID
           AND t_DlContrID = p_DlContrID
           AND t_FIID = one_tick.t_PFI
           AND t_DealID = one_tick.t_DealID
           AND t_OutDate <> p_OutDate;

        IF (v_prevAmount + v_scsumconfexp.T_AMOUNT) = one_tick.t_Principal THEN
          v_scsumconfexp.T_DEALSUM_PAYFI  := one_tick.t_Cost - v_prevDealSum_PayFI;
          v_scsumconfexp.T_NKD_PAYFI      := one_tick.t_NKD - v_prevNKD_PayFI;
          v_scsumconfexp.T_DEALSUM_NAT    := ROUND(RSI_RSB_FIINSTR.ConvSumType(v_totalCost, one_tick.t_CFI, RSI_RSB_FIInstr.NATCUR, 7, v_scsumconfexp.T_SETTLDATE), 2) - v_prevDealSum_Nat;
          v_scsumconfexp.T_SUMCOMISS      := GetCommiss(one_tick.t_BOfficeKind, one_tick.t_DealID, x_SubstDate) - v_prevSumComiss;
          
          v_wasCalc := TRUE;
        END IF;
      END IF;
      
      
      IF v_WasCalc = FALSE THEN
        v_scsumconfexp.T_DEALSUM_PAYFI  := ROUND(v_K*one_tick.t_Cost, 2);
        v_scsumconfexp.T_NKD_PAYFI      := ROUND(v_K*one_tick.t_NKD, 2);
        v_scsumconfexp.T_DEALSUM_NAT    := ROUND(RSI_RSB_FIINSTR.ConvSumType(v_K*v_totalCost, one_tick.t_CFI, RSI_RSB_FIInstr.NATCUR, 7, v_scsumconfexp.T_SETTLDATE), 2);
        v_scsumconfexp.T_SUMCOMISS      := ROUND(v_K*GetCommiss(one_tick.t_BOfficeKind, one_tick.t_DealID, x_SubstDate), 2);
      END IF;

      v_scsumconfexp.T_OUTDATE        := p_OutDate;
                                      
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 't_DealID: '||one_tick.t_DealID||', t_NKD: '||one_tick.t_NKD ) ;
      END IF;
      
      v_scsumconfexp.t_sysdate         := sysdate ;
      
      scsumconfexp_ins.extend;
      scsumconfexp_ins(scsumconfexp_ins.LAST) := v_scsumconfexp;

      v_Rest := v_Rest - v_scsumconfexp.T_AMOUNT;

      IF v_Rest <= 0 THEN
        EXIT;
      END IF;

    END LOOP;

    IF scsumconfexp_ins IS NOT EMPTY THEN
      FORALL i IN scsumconfexp_ins.FIRST .. scsumconfexp_ins.LAST
           INSERT INTO dscsumconfexp_dbt
                VALUES scsumconfexp_ins(i);

      scsumconfexp_ins.delete;
    END IF;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'End' ) ;
    END IF;

  END;

  --Режим = ЦБ на дату
  PROCEDURE CreateRepDataByContr_0_old(p_GUID IN VARCHAR2,
                                   p_DlContrID IN NUMBER,                                                                           
                                   p_OnDate IN DATE,
                                   p_FIID IN NUMBER
                                  )
  AS
    v_PartyID NUMBER;
    
  BEGIN
    
    IF p_DlContrID <= 0 THEN 
      RETURN; 
    END IF;

    SELECT sf.t_PartyID INTO v_PartyID
      FROM ddlcontr_dbt dlcontr, dsfcontr_dbt sf
     WHERE dlcontr.t_DlContrID = p_DlContrID
       AND sf.t_ID = dlcontr.t_SfContrID;

    FOR one_acc IN (SELECT NVL(SUM(q.AccRest), 0) as SumAccRest, q.t_Currency
                      FROM (SELECT ABS(rsb_account.restac(q1.t_Account, q1.t_Currency, p_OnDate, q1.t_Chapter, null)) as AccRest,
                                   q1.t_Currency
                              FROM (SELECT /*+ leading(mp) */ DISTINCT accd.t_Account, accd.t_Chapter, accd.t_Currency
                                      FROM dmcaccdoc_dbt accd, ddlcontrmp_dbt mp
                                     WHERE accd.t_CatID = 364 		-- t_Code = 'ЦБ Клиента, ВУ'
                                       AND accd.t_Owner = v_PartyID
                                       AND accd.t_Currency = (CASE WHEN p_FIID > 0 THEN p_FIID ELSE accd.t_Currency END)
                                       AND accd.t_IsCommon = 'X'
                                       AND accd.t_ActivateDate <= p_OnDate
                                       AND (accd.t_DisablingDate = g_NullDate or accd.t_DisablingDate >= p_OnDate)
                                       AND mp.t_SfContrID = accd.t_ClientContrID
                                       AND mp.t_DlContrID = p_DlContrID
                                   ) q1
                           ) q
                     WHERE q.AccRest > 0
                    GROUP BY q.t_Currency
                   )
    LOOP
      
      CreateDealDataForRest(p_GUID, p_DlContrID, p_OnDate, one_acc.t_Currency, one_acc.SumAccRest, g_NullDate);

    END LOOP;

  END;

  /** 
   @brief    Возвращает полную стоимость
  */
  FUNCTION GetTotalCost ( p_DealID IN NUMBER, p_NKD IN NUMBER )
     RETURN NUMBER deterministic
  IS
    x_TotalCost NUMBER := 1.0;
  BEGIN
    SELECT CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN GetCost ( p_DealID ) + p_NKD
      ELSE leg.t_TotalCost END
    INTO x_TotalCost
    FROM ddl_tick_dbt q_tk 
    INNER JOIN ddl_leg_dbt leg on (leg.t_dealid = q_tk.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
    WHERE q_tk.t_dealid = p_DealID;
    RETURN x_TotalCost;
  END GetTotalCost;

  /** 
   @brief    Возвращает сумму НКД
  */
  FUNCTION GetNKD ( p_DealID IN NUMBER )
     RETURN NUMBER deterministic
  IS
    x_NKD NUMBER := 1.0;
    x_LegNKD NUMBER := 1.0;
  BEGIN
    SELECT CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN 
       NVL((SELECT t_Sum FROM ddlsum_dbt    
            WHERE t_DocKind = RSB_SECUR.DL_AVRWRT AND t_DocID = q_tk.t_DealID AND t_Kind = RSB_SECUR.DLSUM_NKDWRTTAX)
           , 0
       )
       ELSE leg.t_NKD END, leg.t_NKD 
    INTO x_NKD, x_LegNKD
    FROM ddl_tick_dbt q_tk 
    INNER JOIN ddl_leg_dbt leg on (leg.t_dealid = q_tk.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
    WHERE q_tk.t_dealid = p_DealID;
    IF(x_NKD = 0) THEN 
      x_NKD := x_LegNKD;
    END IF;
    RETURN x_NKD;
  END GetNKD;

  /** 
   @brief    Возвращает сумму сделки
  */
  FUNCTION GetCost ( p_DealID IN NUMBER )
     RETURN NUMBER deterministic
  IS
    x_Cost NUMBER := 1.0;
    x_LegCost NUMBER := 1.0;
  BEGIN
    SELECT CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN 
       NVL((SELECT t_Sum FROM ddlsum_dbt    
            WHERE t_DocKind = RSB_SECUR.DL_AVRWRT AND t_DocID = q_tk.t_DealID AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
           , 0
       )
       ELSE leg.t_Cost END, leg.t_Cost
    INTO x_Cost, x_LegCost
    FROM ddl_tick_dbt q_tk 
    INNER JOIN ddl_leg_dbt leg on (leg.t_dealid = q_tk.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
    WHERE q_tk.t_dealid = p_DealID;
    IF(x_Cost = 0) THEN 
      x_Cost := x_LegCost;
    END IF;
    RETURN x_Cost;
  END GetCost;

  /** 
   @brief    Возвращает курс
  */
  FUNCTION GetRate ( p_Sum IN NUMBER, p_FIID IN NUMBER, p_Date IN DATE )
     RETURN NUMBER deterministic
  IS
    x_Rate NUMBER := p_Sum;
  BEGIN
    IF p_FIID <> RSI_RSB_FIInstr.NATCUR THEN
      x_Rate := RSI_RSB_FIINSTR.ConvSumType(p_Sum, p_FIID, RSI_RSB_FIInstr.NATCUR, 7, p_Date);
    END IF;
    RETURN x_Rate;
  END GetRate;

  /** 
   @brief    Возвращает цену сделки
  */
  FUNCTION GetPrice ( p_DealID IN NUMBER )
     RETURN NUMBER deterministic
  IS
    x_Price NUMBER;
    x_LegPrice NUMBER;
  BEGIN
    SELECT CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN
       NVL((SELECT t_Sum FROM ddlsum_dbt    
            WHERE t_DocKind = RSB_SECUR.DL_AVRWRT AND t_DocID = q_tk.t_DealID AND t_Kind = RSB_SECUR.DLSUM_PRICEWRTTAX)
           , 0
       )
       ELSE leg.t_Price END, leg.t_Price
    INTO x_Price, x_LegPrice
    FROM ddl_tick_dbt q_tk 
    INNER JOIN ddl_leg_dbt leg on (leg.t_dealid = q_tk.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
    WHERE q_tk.t_dealid = p_DealID;
    IF(x_Price = 0) THEN 
      x_Price := x_LegPrice;
    END IF;
    RETURN x_Price;
  END GetPrice;

  /** 
   @brief    Возвращает дату получения (дату расчетов)
  */
  FUNCTION GetSettleDate ( p_DealID IN NUMBER )
     RETURN DATE deterministic
  IS
    x_Date DATE;
  BEGIN
    SELECT CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN 
       NVL((SELECT t_Date FROM ddlsum_dbt    
            WHERE t_DocKind = RSB_SECUR.DL_AVRWRT AND t_DocID = q_tk.t_DealID AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
           , leg.t_Start
       )
       WHEN leg.t_MaturityIsPrincipal = 'X' THEN leg.t_Maturity 
       ELSE leg.t_Expiry END 
    INTO x_Date
    FROM ddl_tick_dbt q_tk 
    INNER JOIN ddl_leg_dbt leg on (leg.t_dealid = q_tk.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
    WHERE q_tk.t_dealid = p_DealID;
    RETURN x_Date;
  END GetSettleDate;

  /** 
   @brief    Возвращает дату покупки (дату сделки)
  */
  FUNCTION GetDealDate ( p_DealID IN NUMBER )
     RETURN DATE deterministic
  IS
    x_Date DATE;
  BEGIN
    SELECT 
       CASE WHEN q_tk.t_BOfficeKind <> RSB_SECUR.DL_AVRWRT THEN q_tk.t_DealDate
            WHEN q_tk.t_flag3=chr(88) AND q_tk.t_taxownbegdate != g_NullDate THEN q_tk.t_taxownbegdate
            ELSE NVL((
              SELECT t_Date FROM ddlsum_dbt WHERE t_DocKind = RSB_SECUR.DL_AVRWRT AND t_DocID = q_tk.t_DealID AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX
              ), leg.t_Start) END 
    INTO x_Date
    FROM ddl_tick_dbt q_tk 
    INNER JOIN ddl_leg_dbt leg on (leg.t_dealid = q_tk.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
    WHERE q_tk.t_dealid = p_DealID;
    RETURN x_Date;
  END GetDealDate;

  /** 
   @brief    Возвращает валюту сделки
  */
  FUNCTION GetDealCfi ( p_DealID IN NUMBER )
     RETURN NUMBER deterministic
  IS
    x_Cfi NUMBER;
  BEGIN
    SELECT CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN 
       NVL((SELECT t_Currency FROM ddlsum_dbt 
            WHERE t_DocKind = RSB_SECUR.DL_AVRWRT AND t_DocID = q_tk.t_DealID AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
            , leg.t_CFI
       )
       ELSE leg.t_CFI END
    INTO x_Cfi
    FROM ddl_tick_dbt q_tk 
    INNER JOIN ddl_leg_dbt leg on (leg.t_dealid = q_tk.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
    WHERE q_tk.t_dealid = p_DealID;
    RETURN x_Cfi;
  END GetDealCfi;

  /** 
   @brief    Возвращает дату сделки для курса
  */
  FUNCTION GetDealDateRate ( p_DealID IN NUMBER )
     RETURN DATE deterministic
  IS
    x_Date DATE;
  BEGIN
    SELECT CASE WHEN q_tk.t_BOfficeKind = RSB_SECUR.DL_AVRWRT THEN 
       NVL((SELECT t_Date FROM ddlsum_dbt    
            WHERE t_DocKind = RSB_SECUR.DL_AVRWRT AND t_DocID = q_tk.t_DealID AND t_Kind = RSB_SECUR.DLSUM_COSTWRTTAX)
           , leg.t_Start
       )
       ELSE q_tk.t_DealDate END 
    INTO x_Date
    FROM ddl_tick_dbt q_tk 
    INNER JOIN ddl_leg_dbt leg on (leg.t_dealid = q_tk.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
    WHERE q_tk.t_dealid = p_DealID;
    RETURN x_Date;
  END GetDealDateRate;

  /** 
   @brief    Возвращает Sql-выражение для получения данных отчета в режиме 'ЦБ на дату'
             запрос без применения архива
  */
  FUNCTION GetBuyTicksSql
     RETURN CLOB
  IS
  BEGIN
     RETURN q'[

      -- входные параметры
      WITH prm AS (
         SELECT 
           :t_Fiid AS t_Fiid
           , :t_PartyID AS t_PartyID
         FROM 
           dual
      )

      -- отбор лотов
      , InLots AS (
         SELECT r.t_sumid AS t_BuySumID, SUM(r.t_amount) AS t_amount
           FROM dpmwrtsum_dbt r, prm 
          WHERE r.t_fiid = prm.t_fiid AND r.t_state = 1  -- не поставлен
            AND r.t_amount > 0 AND r.t_portfolio = 0
            AND r.t_party = prm.t_PartyID
         GROUP BY r.t_sumid
      )
    
      -- обогащение сделок информацией
      -- так как в этом режиме отчета нет сделок покупки,
      -- поля t_SaleDealDate, t_SaleDealTime, t_SaleDealID заполняются псевдо-информацией
      SELECT
         TO_DATE('01.01.0001','DD.MM.YYYY') AS t_SaleDealDate, TO_DATE('01.01.0001','DD.MM.YYYY') AS t_SaleDealTime, 1 AS t_SaleDealID -- псевдо-заполнение
         , il.t_BuySumID, bt.t_Dealdate AS t_BuyDealDate, bt.t_DealTime AS t_BuyDealTime, b.t_dealid AS t_BuyDealID, il.t_amount
         , 0 AS t_IsNdfl, RSI_SUMCONFEXP.IsDepoLot(il.t_BuySumID) AS t_IsDepo, RSI_SUMCONFEXP.GetNKD(b.t_dealid) AS t_NKD
         , bt.t_pfi AS t_fiid, bt.t_ClientID AS t_PartyID, RSI_SUMCONFEXP.GetSubstDeal(b.t_dealid) AS t_SubstID, 0 AS t_SubstStatus
         , RSI_SUMCONFEXP.GetParentID(il.t_BuySumID) AS t_ParentID
         , 0 AS t_ConvStatus
         , 0 AS t_IsConv
         , 1 AS t_Numerator
         , 1 AS t_Denominator
         , 6 AS t_SumPrecision
         , leg.t_Principal AS t_Principal
         , bt.t_BOfficeKind AS t_BuyDocKind
         , bt.t_pfi AS t_cur
      FROM InLots il
      INNER JOIN dpmwrtsum_dbt b ON (b.t_sumid = il.t_BuySumID)
      INNER JOIN ddl_tick_dbt bt ON (bt.t_dealid = b.t_dealid)
      INNER JOIN ddl_leg_dbt leg ON (leg.t_dealid = bt.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
      ORDER BY il.t_BuySumID, b.t_changedate
     ]';
  END GetBuyTicksSql;

  /** 
   @brief    Возвращает Sql-выражение для получения данных отчета в режиме 'ЦБ на дату'
             запрос c использованием архива
  */
  FUNCTION GetBuyTicksSql_bc 
     RETURN CLOB
  IS
  BEGIN
     RETURN q'[

      -- входные параметры
      WITH prm AS (
         SELECT 
           :t_Fiid AS t_Fiid
           , :t_PartyID AS t_PartyID
           , :t_OnDate AS t_OnDate
         FROM 
           dual
      )

      -- отбор лотов
      , InLots AS (
         SELECT r.t_sumid AS t_BuySumID, SUM(r.t_amount) AS t_amount
           FROM v_scwrthistex r, prm
          WHERE r.t_fiid = prm.t_fiid AND r.t_state = 1  -- не поставлен
            AND r.t_amount > 0 AND r.t_portfolio = 0
            AND r.t_party = prm.t_PartyID
            AND r.t_ChangeDate < prm.t_OnDate
            AND decode(r.t_Instance, (select max(bc.t_Instance)
                       from v_scwrthistex bc
                       where bc.t_SumID = r.t_SumID
                        and bc.t_ChangeDate < prm.t_OnDate),1,0) = 1
         GROUP BY r.t_sumid
      )
    
      -- обогащение сделок информацией
      SELECT
         TO_DATE('01.01.0001','DD.MM.YYYY') AS t_SaleDealDate, TO_DATE('01.01.0001','DD.MM.YYYY') AS t_SaleDealTime, 1 AS t_SaleDealID -- псевдо-заполнение
         , il.t_BuySumID, bt.t_Dealdate AS t_BuyDealDate, bt.t_DealTime AS t_BuyDealTime, b.t_dealid AS t_BuyDealID, il.t_amount
         , 0 AS t_IsNdfl, RSI_SUMCONFEXP.IsDepoLot(il.t_BuySumID) AS t_IsDepo, RSI_SUMCONFEXP.GetNKD(b.t_dealid) AS t_NKD
         , bt.t_pfi AS t_fiid, bt.t_ClientID AS t_PartyID, RSI_SUMCONFEXP.GetSubstDeal(b.t_dealid) AS t_SubstID, 0 AS t_SubstStatus
         , RSI_SUMCONFEXP.GetParentID(il.t_BuySumID) AS t_ParentID
         , 0 AS t_ConvStatus
         , 0 AS t_IsConv
         , 1 AS t_Numerator
         , 1 AS t_Denominator
         , 6 AS t_SumPrecision
         , leg.t_Principal AS t_Principal
         , bt.t_BOfficeKind AS t_BuyDocKind
         , bt.t_pfi AS t_cur
      FROM InLots il
      INNER JOIN dpmwrtsum_dbt b ON (b.t_sumid = il.t_BuySumID)
      INNER JOIN ddl_tick_dbt bt ON (bt.t_dealid = b.t_dealid)
      INNER JOIN ddl_leg_dbt leg ON (leg.t_dealid = bt.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
      ORDER BY il.t_BuySumID, b.t_changedate
     ]';
  END GetBuyTicksSql_bc;

  /** 
   @brief    Возвращает Sql-выражение для вставки записи в таблицу dmatchticks_tmp
  */
  FUNCTION GetInsMatchTicksTmpSql
     RETURN CLOB
  IS
  BEGIN
     RETURN q'[
      INSERT INTO dmatchticks_tmp r (
         r.t_saledealdate, r.t_saledealtime, r.t_saledealid
         , r.t_buysumid, r.t_buydealdate, r.t_buydealtime, r.t_buydealid, r.t_amount
         , r.t_IsNdfl, r.t_IsDepo, r.t_NKD, r.t_FIID, r.t_PartyID, r.t_SubstID, r.t_SubstStatus
         , r.t_ParentID, r.t_ConvStatus, r.t_IsConv, r.t_Numerator, r.t_Denominator, r.t_SumPrecision
         , r.t_Principal, r.t_BuyDocKind, r.t_Cur, r.t_DepoStatus, r.t_ConvSumID
      ) VALUES (
         :t_saledealdate, :t_saledealtime, :t_saledealid
         , :t_buysumid, :t_buydealdate, :t_buydealtime, :t_buydealid, :t_amount
         , :t_IsNdfl, :t_IsDepo, :t_NKD, :t_FIID, :t_PartyID, :t_SubstID, :t_SubstStatus  
         , :t_ParentID, :t_ConvStatus, :t_IsConv, :t_Numerator, :t_Denominator, :t_SumPrecision
         , :t_Principal, :t_BuyDocKind, :t_Cur, :t_DepoStatus, :t_ConvSumID
      )
     ]';
  END GetInsMatchTicksTmpSql;

  /** 
   @brief    Возвращает Sql-выражение для получения данных отчета в режиме 'Переведенные другому брокеру'
  */
  FUNCTION GetMatchTicksSql ( p_FiidList IN varchar2 )
     RETURN CLOB
  IS
  BEGIN
     RETURN q'[

      -- входные параметры
      WITH prm AS (
         SELECT 
           :t_StartDate AS t_StartDate
           , :t_EndDate AS t_EndDate
           , :t_PartyID AS t_PartyID
           , :t_DlContrID AS t_DlContrID
           , :t_AvrWrt AS t_AvrWrt                        	-- RSB_SECUR.DL_AVRWRT
           , :t_ObjTypeSecdeal AS t_ObjTypeSecdeal              -- RSB_SECUR.OBJTYPE_SECDEAL
         FROM 
           dual
      )

      -- операции списания цб
      , opr AS (
         SELECT t_Kind_Operation, t_DocKind
         FROM doprkoper_dbt, prm 
         WHERE t_DocKind = prm.t_AvrWrt
         AND Rsb_Secur.IsAvrWrtOut(rsb_secur.get_OperationGroup(t_SysTypes)) = 1
      )

      -- отбор сделок списания (продажи)
      , OutTicks AS (
         SELECT tk.t_DealID, tk.t_Dealdate, tk.t_DealTime
           FROM opr, prm, ddl_tick_dbt tk, ddlcontrmp_dbt mp
          WHERE tk.t_BOfficeKind = opr.t_DocKind
            AND tk.t_DealType = opr.t_Kind_Operation
            AND tk.t_ClientID > 0 AND tk.t_ClientID = prm.t_PartyID
            AND tk.t_PFI in (]'||p_FiidList||q'[)
            AND tk.t_DealDate <= prm.t_EndDate AND tk.t_DealDate >= prm.t_StartDate
            AND RSB_SECUR.GetMainObjAttrNoDate(prm.t_ObjTypeSecdeal, LPAD(tk.t_DealID, 34, '0'), 111 /*Внешняя операция*/) = 2 /*Да*/
            AND mp.t_SfContrID = tk.t_ClientContrID AND mp.t_DlContrID = prm.t_DlContrID
            AND tk.t_DealStatus = 20 --Закрыта
      )
      
      -- отбор лотов
      , InLots AS (
        SELECT lnk.t_buyid, lnk.t_saleid, sum(lnk.t_amount) AS t_amount
        FROM OutTicks ot
        INNER JOIN dpmwrtsum_dbt s ON (s.t_dealid = ot.t_dealid)
        INNER JOIN dpmwrtlnk_dbt lnk ON (lnk.t_saleid = s.t_sumid)
        GROUP BY lnk.t_buyid, lnk.t_saleid
      )
    
      -- группировка лотов по сделкам покупки
      , InTicks AS (
        SELECT s.t_dealid AS t_saleid, b.t_sumid AS t_BuySumID, sum(il.t_amount) AS t_amount
        FROM InLots il
        INNER JOIN dpmwrtsum_dbt s ON (s.t_sumid = il.t_saleid)
        INNER JOIN dpmwrtsum_dbt b ON (b.t_sumid = il.t_buyid)
        GROUP BY s.t_dealid, b.t_sumid
      )

      -- обогащение сделок информацией
      SELECT
         st.t_Dealdate AS t_SaleDealDate, st.t_DealTime AS t_SaleDealTime, st.t_DealID AS t_SaleDealID
         , it.t_BuySumID, bt.t_Dealdate AS t_BuyDealDate, bt.t_DealTime AS t_BuyDealTime, b.t_dealid AS t_BuyDealID, it.t_amount
         , 0 AS t_IsNdfl
         , RSI_SUMCONFEXP.IsDepoLot(it.t_BuySumID) AS t_IsDepo
         , RSI_SUMCONFEXP.GetNKD(b.t_dealid) AS t_NKD 
         , bt.t_Pfi AS t_FIID, bt.t_ClientID AS t_PartyID
         , RSI_SUMCONFEXP.GetSubstDeal(b.t_dealid) AS t_SubstID
         , 0 AS t_SubstStatus
         , RSI_SUMCONFEXP.GetParentID(it.t_BuySumID) AS t_ParentID
         , 0 AS t_ConvStatus
         , 0 AS t_IsConv
         , 1 AS t_Numerator
         , 1 AS t_Denominator
         , 6 AS t_SumPrecision
         , leg.t_Principal AS t_Principal
         , bt.t_BOfficeKind AS t_BuyDocKind
         , bt.t_Pfi AS t_Cur
      FROM InTicks it
      INNER JOIN ddl_tick_dbt st ON (st.t_dealid = it.t_saleid)
      INNER JOIN dpmwrtsum_dbt b ON (b.t_sumid = it.t_BuySumID)
      INNER JOIN ddl_tick_dbt bt ON (bt.t_dealid = b.t_dealid)
      INNER JOIN ddl_leg_dbt leg ON (leg.t_dealid = bt.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0)
      ORDER BY st.t_Dealdate, st.t_DealTime, it.t_BuySumID, b.t_changedate
     ]';
  END GetMatchTicksSql;

  /** 
   @brief    Возвращает ID клиента для ДБО
  */
  FUNCTION GetPartyID ( p_DlContrID IN NUMBER )
     RETURN NUMBER
  IS
    x_PartyID NUMBER;
  BEGIN
    SELECT sf.t_PartyID INTO x_PartyID FROM ddlcontr_dbt dlcontr, dsfcontr_dbt sf
      WHERE dlcontr.t_DlContrID = p_DlContrID AND sf.t_ID = dlcontr.t_SfContrID
    ;
    RETURN x_PartyID;
  END GetPartyID;

  /** 
   @brief    Возвращает список валют
  */
  FUNCTION GetFiidList ( p_FIID IN NUMBER )
     RETURN NUMBER
  IS
    x_FiidList VARCHAR2(256);
  BEGIN
    x_FiidList := to_char(p_FIID);
    RETURN x_FiidList;
  END GetFiidList;

  /** 
   @brief    Предыдущие значения части сделки-покупки
  */
  PROCEDURE GetPrev ( 
    p_GUID IN varchar2, p_DealID IN number, p_OutDate IN date
    , p_prevAmount OUT number, p_prevDealSum_PayFI OUT number, p_prevNKD_PayFI OUT number, p_prevDealSum_Nat OUT number, p_prevSumComiss OUT number
  )
  IS
  BEGIN
    RETURN ; -- DEF-78108 следующий запрос выполняется сильно медленно, пока уберем его
    SELECT NVL(SUM(T_AMOUNT), 0), NVL(SUM(T_DEALSUM_PAYFI), 0), NVL(SUM(T_NKD_PAYFI), 0), NVL(SUM(T_DEALSUM_NAT), 0), NVL(SUM(T_SUMCOMISS), 0)    
      INTO p_prevAmount, p_prevDealSum_PayFI, p_prevNKD_PayFI, p_prevDealSum_Nat, p_prevSumComiss
      FROM dscsumconfexp_dbt
     WHERE t_GUID = p_GUID AND t_DealID = p_DealID AND t_OutDate <> p_OutDate
     ;
  END GetPrev;

  /** Возвращает 1, если полученный лот является лотом сделки "зачисления ДЕПО".
      В истории по нему, начальный t_cost будет нулевым.
      В последующем, лоты "зачисления ДЕПО" не обрабатываются.
  */
  FUNCTION IsDepoLot( p_SumID IN NUMBER ) RETURN NUMBER
  IS
    x_Cost NUMBER;
    x_Sql CLOB;
  BEGIN
    IF(p_SumID <> -1) THEN
      -- если не "зачисление НДФЛ" (есть в лотах)
      SELECT r.t_cost INTO x_Cost from v_scwrthistex r where r.T_SUMID = p_SumID and r.t_instance = 0;  
      IF(x_Cost = 0) THEN
        -- если начальный лот с нулевой суммой, значит это лот "зачисления ДЕПО", не обрабатываем такой лот
        RETURN 1;
      END IF;
    END IF;
    RETURN 0;
  EXCEPTION
    WHEN OTHERS THEN 
      -- не может такого быть, чтобы лота не было в истории,
      -- но, на всякий случай, отметим его, как лот DEPO 
      RETURN 0;
  END IsDepoLot;

  /** Возвращает ID родительского лота, если полученный лот является лотом по конвертации.
      Иначе -1.
  */
  FUNCTION GetParentID( p_SumID IN NUMBER ) RETURN NUMBER
  IS
    x_Parent NUMBER;
  BEGIN
    SELECT case when l.t_dockind <> 135 then -1 else l.t_parent end INTO x_Parent FROM dpmwrtsum_dbt l WHERE l.t_SumID = p_SumID;
    RETURN x_Parent;
  EXCEPTION
    WHEN OTHERS THEN 
      RETURN -1;
  END GetParentID;

  /** 
   @brief    Обработка одной сделки покупки
  */
  PROCEDURE ProcessOneBuyDeal (
     p_GUID IN VARCHAR2, p_DlContrID IN NUMBER, p_PartyID IN NUMBER, p_Rec IN t_MatchTickRec, p_Sort IN OUT NUMBER
     , p_BuyDealID IN NUMBER, p_IsConv IN NUMBER, p_Numerator IN NUMBER, p_Denominator IN NUMBER, p_SumPrecision IN NUMBER
     , p_Amount IN NUMBER, p_BuyDealDate IN DATE, p_DealTime IN DATE, p_Pfi IN NUMBER, p_Principal IN NUMBER, p_BuyDocKind IN NUMBER
     , p_RepDate IN DATE
  )
  AS
    x_FiidList VARCHAR2(256);
    x_buyDealDate DATE;  		-- Дата сделки для курса
    x_buyDealDateRate DATE;  		-- Дата сделки для курса
    x_Cost NUMBER := 0;			-- Сумма сделки
    x_NKD NUMBER := 0;			-- Сумма НКД
    x_TotalCost NUMBER := 0;            -- полная стоимость
    x_Cfi NUMBER := 0;			-- Валюта сделки
    x_SettlDate DATE;			-- дата получения (дата расчетов)
    x_Amount NUMBER := p_Amount; 	-- сумма лота
    x_Principal NUMBER := p_Principal; 	-- сумма покупки
    x_Price NUMBER;	        	-- цена сделки
    x_FaceValue NUMBER;                 -- Номинал в валюте ЦБ
    x_DealSumPayFI NUMBER;		-- Сумма сделки в валюте расчетов
    x_NkdPayFI NUMBER;			-- НКД в валюте номинала
    x_DealSumNat NUMBER;		-- Сумма сделки в рублях
    x_SumComiss NUMBER;			-- Комиссионные затраты
    x_DealDateRate NUMBER;		-- курс на дату сделки
    x_SettlDateRate NUMBER;		-- курс на дату расчетов
    x_SubstDate DATE;                   -- дата замещения (то есть начальная дата приобретения при сделке замещения)

    x_prevAmount NUMBER := 0;
    x_prevDealSum_PayFI NUMBER := 0;
    x_prevNKD_PayFI NUMBER := 0;
    x_prevDealSum_Nat NUMBER := 0;
    x_prevSumComiss NUMBER := 0;
    x_Cnt NUMBER;

    v_scsumconfexp   DSCSUMCONFEXP_DBT%rowtype;
  BEGIN
    p_Sort := p_Sort + 1;

    IF(p_IsConv = 0) THEN
      -- если не конвертация, то сумма лота придет в p_Rec
      x_Amount := p_Rec.t_Amount;
    END IF;

    x_BuyDealDate	:= RSI_SUMCONFEXP.GetDealDate(p_BuyDealID);  				        -- дата покупки (дата сделки)
    x_SumComiss         := GetCommiss(p_BuyDocKind, p_BuyDealID, x_SubstDate);

    x_FaceValue         := RSI_RSB_FIInstr.FI_GetNominalOnDate(p_PFI, x_BuyDealDate);
    x_Cfi           	:= RSI_SUMCONFEXP.GetDealCfi(p_BuyDealID);  				        -- валюта сделки
    x_SettlDate		:= RSI_SUMCONFEXP.GetSettleDate(p_BuyDealID);  				        -- дата получения (дата расчетов)
    IF(x_SubstDate IS NOT NULL) THEN
      x_SettlDate := x_SubstDate;
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'x_SettlDate: '||x_SettlDate||', было замещение, замена на дату первоприобретения' ) ;
      END IF;
    END IF;

    x_buyDealDateRate   := RSI_SUMCONFEXP.GetDealDateRate(p_BuyDealID);
    x_DealDateRate	:= RSI_SUMCONFEXP.GetRate(1.0, x_Cfi, x_buyDealDateRate);       		-- курс на дату сделки
    x_SettlDateRate     := RSI_SUMCONFEXP.GetRate(1.0, x_Cfi, x_SettlDate);      			-- курс на дату расчетов
    x_Price             := RSI_SUMCONFEXP.GetPrice(p_BuyDealID);					-- цена сделки

    x_Cost              := RSI_SUMCONFEXP.GetCost(p_BuyDealID);					        -- сумма сделки
    x_NKD               := p_Rec.t_NKD;					                                -- сумма НКД (возьмём с лота)
    x_TotalCost         := RSI_SUMCONFEXP.GetTotalCost(p_BuyDealID, x_NKD);		        	-- полная стоимость

    -- Если конвертация, переводим суммы в валюту p_FIID
    IF(p_IsConv = 1) THEN
       -- DEF-84364 x_Cost := ROUND( x_Cost * p_Numerator / p_Denominator , p_SumPrecision ); 
       x_NKD := ROUND( x_NKD * p_Numerator / p_Denominator , p_SumPrecision );
       -- DEF-84364 x_TotalCost := ROUND( x_TotalCost * p_Numerator / p_Denominator , p_SumPrecision );
       x_Principal := ROUND( x_Principal * p_Numerator / p_Denominator , p_SumPrecision );
       x_Amount := ROUND( x_Amount * p_Numerator / p_Denominator , p_SumPrecision );
       IF (p_Numerator <> 0) THEN
         x_Price := ROUND( x_Price * p_Denominator / p_Numerator, p_SumPrecision ); -- DEF-84364, пересчет цены при конвертации
       END IF;
    END IF;

    -- DEF-92673 почему-то вычислялись неверно, дефект идет в моно-релиз, разбираться некогда
    x_DealSumPayFI  := ROUND(x_Cost, 2);							-- Сумма сделки в валюте расчетов
    x_NkdPayFI      := ROUND(x_NKD, 2); 							-- НКД в валюте номинала
    x_DealSumNat    := ROUND(RSI_SUMCONFEXP.GetRate(x_TotalCost, x_Cfi, x_SettlDate), 2); 	-- Сумма сделки в рублях
    x_SumComiss     := ROUND(x_SumComiss, 2); 							-- Комиссионные затраты

    INSERT INTO dscsumconfexp_dbt r (
      r.t_guid, r.t_clientid, r.t_dlcontrid, r.t_fiid, r.t_sort, r.t_dealid, r.t_dealdate, r.t_dealdate_rate
      , r.t_settldate, r.t_settldate_rate, r.t_amount, r.t_cfi, r.t_price, r.t_facevalue, r.t_dealsum_payfi
      , r.t_nkd_payfi, r.t_dealsum_nat, r.t_sumcomiss, r.t_outdate, r.t_repdate
    ) VALUES (
      p_GUID, p_Rec.t_PartyID, p_DlContrID, p_Rec.t_Cur, p_Sort, p_BuyDealID, x_BuyDealDate, x_DealDateRate
      , x_SettlDate, x_SettlDateRate, x_Amount, x_Cfi, x_Price, x_FaceValue, x_DealSumPayFI
      , x_NkdPayFI, x_DealSumNat, x_SumComiss, p_Rec.t_SaleDealDate, p_RepDate
    );

  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Exception: t_clientid: '||p_Rec.t_PartyID||', t_dlcontrid: '||p_DlContrID
           ||', t_fiid: '||p_Rec.t_FIID||', t_outdate: '||p_Rec.t_SaleDealDate||', t_sort: '||p_Sort
        ) ;
      END IF;
  END ProcessOneBuyDeal;

  /** 
   @brief    Получение данных отчета для обоих режимов (принимает курсор)
  */
  PROCEDURE CreateRepData (
     p_GUID IN VARCHAR2
     , p_DlContrID IN NUMBER
     , p_PartyID IN NUMBER
     , p_OnDate IN DATE
     , p_Cnt OUT NUMBER
  )
  AS
    x_Sql CLOB;
    x_Sort NUMBER := 0;
    x_Rec t_MatchTickRec;
    x_Prefix VARCHAR2(64) := '';
    x_Result NUMBER := 0;
    x_StartTime pls_integer := dbms_utility.get_time;
    x_Cursor SYS_REFCURSOR;
  BEGIN
    -- проход по курсору, заполнение массива, 
    -- сделки замещения (а также лоты конвертации и сделки зачисления ДЕПО) к этому моменту должны быть заменены на соответствующие им лоты
    -- поэтому убираем их из запроса (по условиям
    -- t_SubstID = -1 -- для сделок замещения
    -- t_ParentID = -1 -- для лотов конвертации
    -- t_IsDepo = 0 -- для сделок зачисления ДЕПО)
    x_Sql := q'[
       SELECT 
         r.t_saledealdate, r.t_saledealtime, r.t_saledealid
         , r.t_buysumid, r.t_buydealdate, r.t_buydealtime, r.t_buydealid, r.t_amount 
         , r.t_IsNdfl, r.t_IsDepo, r.t_NKD, r.t_FIID, r.t_PartyID, r.t_SubstID, r.t_SubstStatus
         , r.t_ParentID, r.t_ConvStatus, r.t_IsConv, r.t_Numerator, r.t_Denominator, r.t_SumPrecision
         , r.t_Principal, r.t_BuyDocKind, r.t_Cur
       FROM 
         dmatchticks_tmp r
       WHERE
         r.t_SubstID = -1 AND r.t_ParentID = -1 
         AND r.t_IsSkip = 0				-- DEF-81824
       ORDER BY 
         r.t_Cur
         -- DEF-101246 Сортировка изменена для вывода сделок в отчете по порядку FIFO
         , r.t_buydealdate ASC, r.t_buydealtime ASC, r.t_buydealid		
    ]';
    p_Cnt := 0;
    OPEN x_Cursor FOR x_Sql;
    LOOP
      FETCH x_Cursor INTO x_Rec;
      EXIT WHEN x_Cursor%NOTFOUND;

      ProcessOneBuyDeal(
        p_GUID, p_DlContrID, p_PartyID, x_Rec, x_Sort
        , x_Rec.t_BuyDealID, x_Rec.t_IsConv, x_Rec.t_Numerator, x_Rec.t_Denominator, x_Rec.t_SumPrecision
        , x_Rec.t_Amount, x_Rec.t_BuyDealDate, x_Rec.t_BuyDealTime, x_Rec.t_FIID, x_Rec.t_Principal, x_Rec.t_BuyDocKind
        , p_OnDate
      );

      p_Cnt := p_Cnt + 1;
    END LOOP;
    CLOSE x_Cursor;
    COMMIT;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'x_Sort: '||x_Sort||', p_Cnt: '||p_Cnt ) ;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END CreateRepData;

  /** 
   @brief    Для финансового инструмента (p_FIID) и клиента (p_PartyID)
             Возвращает максимальную дату изменения лота
  */
  FUNCTION GetMaxChangeDate ( p_FIID IN NUMBER, p_PartyID IN NUMBER )
     RETURN DATE
  IS
    x_MaxChangeDate DATE := TO_DATE ('01.01.0001', 'DD.MM.YYYY');
  BEGIN
    SELECT NVL(max(s.t_changedate), TO_DATE ('01.01.0001', 'DD.MM.YYYY')) AS MaxChangeDate
      INTO x_MaxChangeDate
      FROM dpmwrtsum_dbt s
     WHERE s.t_fiid = p_FIID AND s.t_Party = p_PartyID
    ;
    RETURN x_MaxChangeDate;
  EXCEPTION WHEN OTHERS THEN 
    RETURN x_MaxChangeDate;
  END GetMaxChangeDate;

  /** 
   @brief    К полученной сделке зачисления определяет ID сделки списания (если такая есть, фактически, это -- перевод между суб-счетами)
             Если сделки списания нет, возвращает NULL.
             Доработка по DEF-89639: может быть несколько сделок списания по нужной сумме, поэтому надо определять еще и по дате
  */
  FUNCTION GetOutDealID ( p_InDealID IN NUMBER )
     RETURN NUMBER
  IS
    x_OutDealID NUMBER;  -- возвращаемое значение
    x_Pfi number;
    x_ClientID number;
    x_DlContrID number;
    x_Principal number;
    x_DealDate date;
  BEGIN
    -- реквизиты сделки зачисления 
    SELECT r.t_pfi, r.t_clientid, m.t_dlcontrid, l.t_principal, r.t_dealDate
      INTO x_Pfi, x_ClientID, x_DlContrID, x_Principal, x_DealDate
      FROM ddl_tick_dbt r 
      JOIN dsfcontr_dbt s ON (s.t_id = r.t_clientcontrid)
      JOIN ddlcontrmp_dbt m ON (m.t_sfcontrid = r.t_clientcontrid)
      JOIN ddl_leg_dbt l ON (l.t_dealid = r.t_dealid AND l.t_legkind = 0 AND l.t_legid  = 0)
      WHERE r.t_dealid = p_InDealID and r.t_dealtype = 2011
    ;
    -- поиск сделки списания
    SELECT r.t_dealid INTO x_OutDealID
      FROM ddl_tick_dbt r 
      JOIN dsfcontr_dbt s ON (s.t_id = r.t_clientcontrid)
      JOIN ddlcontrmp_dbt m ON (m.t_sfcontrid = r.t_clientcontrid)
      JOIN ddl_leg_dbt l ON (l.t_dealid = r.t_dealid AND l.t_legkind = 0 AND l.t_legid  = 0)
      WHERE r.t_pfi = x_Pfi and r.t_clientid = x_ClientID and r.t_dealtype = 2010
      and m.t_dlcontrid = x_DlContrID and l.t_principal = x_Principal and r.t_dealDate = x_DealDate
      -- DEF-98100
      and r.t_flag3 <> 'X'
    ;
    RETURN x_OutDealID;
  EXCEPTION WHEN OTHERS THEN 
    RETURN null;
  END GetOutDealID;

  /** 
   @brief    Возвращает 0, если нет необработанных сделок зачисления ДЕПО.
             Если есть необработанные зачисления ДЕПО, возвращает количество.
  */
  FUNCTION ExistDepo
     RETURN NUMBER
  IS
    x_Cnt NUMBER;
  BEGIN
    SELECT COUNT(*) INTO x_Cnt FROM dmatchticks_tmp r WHERE r.t_isdepo = 1 and r.t_isconv = 0 AND r.t_depoStatus = 0;
    RETURN x_Cnt;
  END ExistDepo;


  /** 
   @brief    Возвращает 0, если нет необработанных сделок замещения.
             Если есть необработанные сделки замещения, возвращает количество.
  */
  FUNCTION ExistSubst
     RETURN NUMBER
  IS
    x_Cnt NUMBER;
  BEGIN
    SELECT COUNT(*) INTO x_Cnt FROM dmatchticks_tmp r WHERE r.t_substID <> -1 AND r.t_substStatus = 0;
    RETURN x_Cnt;
  END ExistSubst;


  /** 
   @brief    Возвращает 0, если нет необработанных лотов конвертации.
             Если есть необработанные лоты конвертации, возвращает количество.
  */
  FUNCTION ExistConv
     RETURN NUMBER
  IS
    x_Cnt NUMBER;
  BEGIN
    SELECT COUNT(*) INTO x_Cnt FROM dmatchticks_tmp r WHERE r.t_parentID <> -1 AND r.t_convStatus = 0;
    RETURN x_Cnt;
  END ExistConv;


  /** 
   @brief    Процедура обработки необработанных сделок замещения.
  */
  PROCEDURE ProcessSubst
  AS
    x_Sql CLOB;
    x_OutDealID NUMBER;  	-- здесь определяется ID сделки списания (если есть пара зачисления/списания, фактически перевод между суб-счетами)
    x_SubstID NUMBER;  		-- здесь определяется ID сделки замещения
    x_SubstFIID NUMBER;  	-- ФИ сделки замещения
    x_SubstPartyID NUMBER;  	-- ID клиента сделки замещения
    x_SubstDate DATE;  		-- дата сделки замещения
    x_FIID NUMBER;
    x_PartyID NUMBER;
    x_DealID NUMBER;
    x_DealDate DATE;  		-- дата сделки замещения
    x_Cur NUMBER;
  BEGIN
    x_Sql := GetInsMatchTicksTmpSql();		
    FOR i IN (
       SELECT r.t_substID, r.t_saledealdate, r.t_saledealtime, r.t_saledealid, r.t_FIID
         FROM dmatchticks_tmp r 
         WHERE r.t_substID <> -1 AND r.t_substStatus = 0
    ) LOOP
      x_Cur := i.t_FIID;
      -- Получаем инфу о сделке замещения
      GetSubstInfo(i.t_substID, x_SubstFIID, x_SubstPartyID, x_SubstDate);
      -- Если в лотах отсутствуют "Зачисления НДФЛ", соберем их отдельно
      GetNdflTicks( i.t_saledealdate, i.t_saledealtime, i.t_saledealid, x_SubstPartyID, x_SubstFIID, x_Cur );
      -- Смотрим лоты сделки замещения
      FOR j IN (
         SELECT 
           b.t_sumid, b.t_dealid, t.t_dealdate, t.t_dealtime, e.t_principal, t.t_pfi AS t_FIID
           , t.t_clientID AS t_PartyID, t.t_BOfficeKind AS t_BuyDocKind
           FROM dpmwrtsum_dbt s 
           JOIN dpmwrtlnk_dbt l ON (l.t_saleid = s.t_sumID) 
           JOIN dpmwrtsum_dbt b ON (b.t_sumid = l.t_buyID)
           JOIN ddl_tick_dbt t ON (t.t_dealid = b.t_dealid) 
           JOIN ddl_leg_dbt e ON (e.t_dealid = t.t_dealid AND e.t_legkind = 0 AND e.t_legid  = 0)
           WHERE s.t_dealid = i.t_substID
      ) LOOP
         x_OutDealID := GetOutDealID( j.t_dealid );
         IF(x_OutDealID IS NULL) THEN 
           -- это не cделка перевода между суб-договорами,
           -- отправляем в таблицу, как есть
           x_DealID := RSI_SUMCONFEXP.GetSubstDeal( j.t_dealid );
           IF( x_DealID <> -1 ) THEN
             -- сделка замещения
             GetSubstInfo(x_DealID, x_FIID, x_PartyID, x_DealDate);
           ELSE
             -- не сделка замещения
             x_FIID := j.t_FIID;
             x_PartyID := j.t_PartyID;
             x_DealDate := j.t_dealdate;
           END IF;
           EXECUTE IMMEDIATE x_Sql USING 
              i.t_saledealdate, i.t_saledealtime, i.t_saledealid
              , j.t_sumid, j.t_dealdate, j.t_dealtime, j.t_dealid, j.t_principal
              , 0, RSI_SUMCONFEXP.IsDepoLot( j.t_sumid ), RSI_SUMCONFEXP.GetNKD( j.t_dealid )
              , x_FIID, x_PartyID, x_DealID, 0
              , RSI_SUMCONFEXP.GetParentID( j.t_sumid ), 0, 0, 1, 1, 6
              , j.t_principal, j.t_BuyDocKind, x_Cur, 0, 0
           ;
         ELSE
           -- это сделка перевода между суб-договорами, ищем исходные лоты приобретения
           FOR k IN (
              SELECT 
                b.t_sumid, b.t_dealid, t.t_dealdate, t.t_dealtime, e.t_principal, t.t_pfi AS t_FIID
                , t.t_clientID AS t_PartyID, t.t_BOfficeKind AS t_BuyDocKind
                FROM dpmwrtsum_dbt s 
                JOIN dpmwrtlnk_dbt l ON (l.t_saleid = s.t_sumID) 
                JOIN dpmwrtsum_dbt b ON (b.t_sumid = l.t_buyID)
                JOIN ddl_tick_dbt t ON (t.t_dealid = b.t_dealid) 
                JOIN ddl_leg_dbt e ON (e.t_dealid = t.t_dealid AND e.t_legkind = 0 AND e.t_legid  = 0)
                WHERE s.t_dealid = x_OutDealID
           ) LOOP
              -- направляем в таблицу найденные лоты (которые, фактически, подменяют сделку перевода между суб-счетами)
              x_DealID := RSI_SUMCONFEXP.GetSubstDeal( k.t_dealid );
              IF( x_DealID <> -1 ) THEN
                -- сделка замещения
                GetSubstInfo(x_DealID, x_FIID, x_PartyID, x_DealDate);
              ELSE
                -- не сделка замещения
                x_FIID := k.t_FIID;
                x_PartyID := k.t_PartyID;
                x_DealDate := k.t_dealdate;
              END IF;
              EXECUTE IMMEDIATE x_Sql USING 
                 i.t_saledealdate, i.t_saledealtime, i.t_saledealid
                 , k.t_sumid, k.t_dealdate, k.t_dealtime, k.t_dealid, k.t_principal
                 , 0, RSI_SUMCONFEXP.IsDepoLot( k.t_sumid ), RSI_SUMCONFEXP.GetNKD( k.t_dealid )
                 , x_FIID, x_PartyID, x_DealID, 0
                 , RSI_SUMCONFEXP.GetParentID( k.t_sumid ), 0, 0, 1, 1, 6
                 , k.t_principal, k.t_BuyDocKind, x_Cur, 0, 0
              ;
           END LOOP;
         END IF;
      END LOOP;
      -- Помечаем сделку замещения, как обработанную
      UPDATE dmatchticks_tmp m 
        SET m.t_substStatus = 1, m.t_fiid = x_SubstFIID, m.t_PartyID = x_SubstPartyID
        WHERE m.t_substID = i.t_substID;
      COMMIT;
    END LOOP;
  END ProcessSubst;

  /** 
   @brief    Функция возвращает sql-выражение для получения сделок "зачисления НДФЛ"
             По-правильному, ID сделок "зачисления НДФЛ" должен быть больше, чем ID сделки "зачисления ДЕПО"
             Но иногда это не так (см. AVT_BOSS-573_AVT_BOSS-1356), 
             Поэтому при p_Num = 1 (первая попытка) выражение строится с условием по ID сделки ДЕПО,
             И, если сделки зачисления НДФЛ, получить не удалось, предпринимается вторая попытка (p_Num = 2),
             и сделки зачисления НДФЛ вытаскиваются без учета ID сделки ДЕПО.
  */
  FUNCTION getNdflSql(p_Num number) RETURN Clob IS
    x_Sql clob;
    x_And varchar2(64);
  BEGIN
    if(p_Num = 1) then
      x_And := ' AND t.t_dealid > :p_DepoDealID ';
    end if;
    x_Sql := q'[
      SELECT 
        sm.t_date AS t_BuyDealDate, t.t_dealtime AS t_BuyDealTime, t.t_dealid AS t_BuyDealID, l.t_principal AS t_amount
        , RSI_SUMCONFEXP.GetNKD(t.t_dealid) AS t_NKD
        , l.t_Principal AS t_Principal
        , t.t_BOfficeKind AS t_BuyDocKind
      FROM ddl_tick_dbt t
      JOIN ddl_leg_dbt l ON (t.t_dealid = l.t_Dealid and l.t_legkind = 0 and l.t_legid = 0)
      JOIN dobjatcor_Dbt a ON (A.T_GROUPID = 210 and A.T_OBJECTTYPE = 101 and a.t_object = lpad(t.t_dealid, 34,'0'))
      LEFT JOIN dpmwrtsum_dbt s ON (s.t_dealid = t.t_dealid)
      LEFT JOIN ddlsum_dbt sm ON ( sm.t_dockind = t.t_bofficekind and sm.t_docid = t.t_dealid and sm.t_kind = 1220 )
      WHERE t.t_clientid = :p_PartyID and t.t_pfi in (:p_FIID, :p_Cur, :x_DepoPfi)  -- DEF-101302, AVT_BOSS-1354
      AND s.t_dealid is null]'
      ||x_And
      ||q'[  
      ORDER BY sm.t_date DESC, t.t_dealid DESC
    ]';
    RETURN x_Sql;
  END getNdflSql;

  /** 
   @brief    Процедура обработки необработанных сделок "зачисления ДЕПО".
  */
  PROCEDURE ProcessNdfl ( 
    p_DepoDealID IN NUMBER, p_PartyID IN NUMBER, p_FIID IN NUMBER, p_Amount IN NUMBER
    , p_SaleDealDate IN DATE, p_SaleDealTime IN DATE, p_SaleDealId IN NUMBER, p_Cur IN NUMBER
    , p_CntNdfl OUT NUMBER, p_SumNdfl OUT NUMBER
  )
  AS
    x_Sql CLOB;
    x_Amount NUMBER := p_Amount;  		-- Сумма сделки "зачисления ДЕПО"
    x_Sum NUMBER;
    x_CntNdfl NUMBER;
    x_DepoPfi NUMBER;
    x_Rec t_NdflRec;
    x_Cursor SYS_REFCURSOR;
  BEGIN
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'Start ProcessNdfl(), x_Amount: '||x_Amount||', p_DepoDealID: '||p_DepoDealID ) ;
    END IF;
    IF( x_ViewedDepo.exists( p_DepoDealID ) ) THEN
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Exist p_DepoDealID: '||p_DepoDealID ) ;
      END IF;
      p_CntNdfl := x_ViewedDepo( p_DepoDealID );
      p_SumNdfl := p_Amount;
    ELSE
      SELECT l.t_pfi INTO x_DepoPfi FROM ddl_leg_dbt l  -- AVT_BOSS-1354 Определяем валюту сделки "зачисления ДЕПО"
        WHERE l.t_Dealid = p_DepoDealID and l.t_legkind = 0 and l.t_legid = 0;
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Not Exist p_DepoDealID: '||p_DepoDealID||', p_FIID:'||p_FIID||', p_Cur:'||p_Cur ) ;
      END IF;
      x_CntNdfl := 0;

      -- Предпринимаются две попытки вытащить сделки зачисления НДФЛ
      FOR k IN 1..2 LOOP
        x_Sql := getNdflSql(k);
        p_SumNdfl := 0;		-- AVT_BOSS-1356 сумма сделок "зачисления НДФЛ"
        IF(k = 1) THEN
          OPEN x_Cursor FOR x_Sql USING p_PartyID, p_FIID, p_Cur, x_DepoPfi, p_DepoDealID;
        ELSE
          OPEN x_Cursor FOR x_Sql USING p_PartyID, p_FIID, p_Cur, x_DepoPfi;
        END IF;
        LOOP
          FETCH x_Cursor INTO x_Rec;
          EXIT WHEN x_Cursor%NOTFOUND;

          IF(g_DebugFlag = 1) THEN
            it_log.log( p_msg => 'x_Rec.t_BuyDealID: '||x_Rec.t_BuyDealID
              ||', x_Rec.t_amount: '||x_Rec.t_amount
            ) ;
          END IF;
          x_CntNdfl := x_CntNdfl + 1;
          IF(x_Amount >= x_Rec.t_amount) THEN
            -- Добавляем сделку "зачисления НДФЛ", и можно переходить к следующей
            x_Amount := x_Amount - x_Rec.t_amount; 
            x_Sum := x_Rec.t_amount;
          ELSE
            -- Эта сделка "зачисления НДФЛ" частично закрывает , она будет последней из добавленных
            x_Sum := x_Amount;  -- DEF-103125
            x_Amount := 0;
          END IF;
          p_SumNdfl := p_SumNdfl + x_Sum;    	-- AVT_BOSS-1356 Считаем сумму сделок "зачисления НДФЛ"
          -- Вставка текущей сделки "зачисления НДФЛ"
          INSERT INTO dmatchticks_tmp r (
             r.t_saledealdate, r.t_saledealtime, r.t_saledealid
             , r.t_buysumid, r.t_buydealdate, r.t_buydealtime, r.t_buydealid, r.t_amount
             , r.t_IsNdfl, r.t_IsDepo, r.t_NKD
             , r.t_FIID, r.t_PartyID, r.t_SubstID, r.t_SubstStatus
             , r.t_ParentID, r.t_ConvStatus, r.t_IsConv, r.t_Numerator, r.t_Denominator, r.t_SumPrecision
             , r.t_Principal, r.t_BuyDocKind, r.t_Cur, r.t_depoStatus
          ) VALUES (
             p_SaleDealDate, p_SaleDealTime, p_SaleDealId
             , -1 					-- t_buysumid
             , x_Rec.t_buydealdate
             , x_Rec.t_buydealtime
             , x_Rec.t_buydealid
             , x_Sum
             , 1         					-- t_IsNdfl
             , 0						-- t_IsDepo
             , x_Rec.t_NKD
             , p_FIID					-- t_FIID
             , p_PartyID					-- t_PartyID
             , -1  					-- t_SubstID
             , 1   					-- t_SubstStatus
             , -1						-- t_ParentID
             , 1   					-- t_ConvStatus
             , 0  					-- t_IsConv
             , 1						-- t_Numerator
             , 1 						-- t_Denominator
             , 6						-- t_SumPrecision
             , x_Sum 					-- t_Principal
             , x_Rec.t_BuyDocKind
             , g_Cur   					-- t_Cur
             , 0						-- t_depoStatus
          );
          -- останавливаемся, если распределена вся сумма текущей сделки "зачисления ДЕПО"
          IF(x_Amount <= 0) THEN
            EXIT;
          END IF;
        END LOOP; -- x_Cursor
        CLOSE x_Cursor;

        IF(x_CntNdfl > 0) THEN
          EXIT;
        END IF;
      END LOOP;  -- k

      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'x_CntNdfl: '||x_CntNdfl ) ;
      END IF;
      x_ViewedDepo( p_DepoDealID ) := x_CntNdfl; -- DEF-98100, помечаем сделку, как обработанную, во избежание дублирования
      p_CntNdfl := x_CntNdfl;
    END IF;
  END ProcessNdfl;

  /** 
   @brief    Обновление параметров конвертации для сделок "зачислений НДФЛ"
  */
  PROCEDURE UpdateNdflConv ( p_PartyID IN NUMBER, p_FIID IN NUMBER, p_Numerator IN NUMBER, p_Denominator IN NUMBER, p_SumPrecision IN NUMBER  )
  AS
  BEGIN
    UPDATE dmatchticks_tmp r 
       SET r.t_Numerator = p_Numerator, r.t_Denominator = p_Denominator, r.t_SumPrecision = p_SumPrecision, r.t_IsConv = 1
     WHERE r.t_PartyID = p_PartyID AND r.t_FIID = p_FIID and r.t_IsNdfl = 1
    ;
  END UpdateNdflConv;

  /** 
   @brief    Процедура обработки необработанных сделок "зачисления ДЕПО".
  */
  PROCEDURE ProcessDepo
  AS
    x_BuyDealID NUMBER;  	-- ID сделки "зачисления ДЕПО"
    x_Amount NUMBER;  		-- Сумма сделки "зачисления ДЕПО"
    x_Sum NUMBER;
    x_IsSkip NUMBER;
    x_CntNdfl NUMBER;
    x_SumNdfl NUMBER;
  BEGIN
    FOR i IN (
       SELECT 
         r.t_saledealdate, r.t_saledealtime, r.t_saledealid
         , r.t_buydealid, r.t_amount, r.t_fiid, r.t_partyid
         FROM dmatchticks_tmp r 
         WHERE r.t_isdepo = 1 AND r.t_ParentID = -1 AND r.t_depoStatus = 0
    ) LOOP
      x_BuyDealID := i.t_buydealid;
      x_IsSkip := 0;
      x_CntNdfl := 0;
      -- Если сделка ДЕПО еще не обработана, обрабатываем
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'x_BuyDealID: '||x_BuyDealID||', i.t_amount: '||i.t_amount||', i.t_partyid: '||i.t_partyid||', i.t_fiid: '||i.t_fiid ) ;
      END IF;
      IF(g_DepoProcessed = 1) THEN
         -- если был произведен разнос сделок зачисления ДЕПО, то больше его не делаем 
         x_SumNdfl := 0;
      ELSE  
        ProcessNdfl (
           x_BuyDealID, i.t_partyid, i.t_fiid, i.t_amount
           , i.t_saledealdate, i.t_saledealtime, i.t_saledealid, i.t_fiid
           , x_CntNdfl, x_SumNdfl
        );
        g_DepoProcessed := 1;
        UpdateNdflConv( i.t_partyid, i.t_fiid, g_Numerator, g_Denominator, 6 );
      END IF;
      -- Если была произведена вставка сделок "зачисления НДФЛ" на всю сумму, сделка "Зачисления ДЕПО" помечается, как неиспользуемая
      IF(x_SumNdfl = i.t_amount) THEN
        x_IsSkip := 1;
        IF(g_DebugFlag = 1) THEN
          it_log.log( p_msg => 'DEPO закрыто полностью' ) ;
        END IF;
      ELSE
        -- AVT_BOSS-1356 корректируем сумму сделки зачисления ДЕПО на нераспределенный остаток
        x_SumNdfl := i.t_amount - x_SumNdfl;  
        IF(g_DebugFlag = 1) THEN
          it_log.log( p_msg => 'DEPO не закрыто на сумму '||x_SumNdfl ) ;
        END IF;
      END IF;
      -- Помечаем сделку "зачисления ДЕПО", как обработанную
      UPDATE dmatchticks_tmp m 
        SET m.t_depoStatus = 1, m.t_isSkip = x_IsSkip, m.t_amount = x_SumNdfl
        WHERE m.t_buydealid = x_BuyDealID AND m.t_isdepo = 1;
      COMMIT;
    END LOOP;
  END ProcessDepo;

  /** 
   @brief    Возвращает 1, если есть необработанный лот по конверации, иначе возвращает 0
             Также в параметрах возвращается инфа о лоте конвертации, необходимая для дальнейшей
             обработки
  */
  FUNCTION GetConvStatus0( 
     p_BuySumID OUT NUMBER, p_ParentID OUT NUMBER, p_SaleDealDate OUT DATE, p_SaleDealTime OUT DATE, p_SaleDealID OUT NUMBER
     , p_Cur OUT NUMBER, p_IsDepo OUT NUMBER, p_BuyDealID OUT NUMBER, p_Amount OUT NUMBER
  ) RETURN NUMBER
  IS
    x_ConvSumID NUMBER;  -- AVT_BOSS-2262 Если заполнен, то используется он
  BEGIN
    SELECT r.t_BuySumID, r.t_parentID, r.t_saledealdate, r.t_saledealtime, r.t_saledealid, r.t_FIID, r.t_IsDepo, r.t_BuyDealID, r.t_Amount, r.t_ConvSumID
      INTO p_BuySumID, p_ParentID, p_SaleDealDate, p_SaleDealTime, p_SaleDealID, p_Cur, p_IsDepo, p_BuyDealID, p_Amount, x_ConvSumID
      FROM dmatchticks_tmp r 
      WHERE r.t_parentID <> -1 AND r.t_convStatus = 0 AND ROWNUM = 1
    ;
    IF(NVL(x_ConvSumID, 0) <> 0) THEN
      p_BuySumID := x_ConvSumID;
    END IF;
    RETURN 1;
  EXCEPTION
    WHEN OTHERS THEN 
      RETURN 0;
  END GetConvStatus0;

  /** 
   @brief    Процедура обработки необработанных лотов конвертации.
  */
  PROCEDURE ProcessConv
  AS
    x_Sql CLOB;
    x_BuySumID NUMBER;		-- лот-конвертации
    x_ParentID NUMBER;		-- родитель лота-конвертации
    x_SaleDealDate DATE;	-- дата сделки продажи
    x_SaleDealTime DATE;	-- время сделки продажи
    x_SaleDealID NUMBER;	-- ID сделки продажи
    x_OutDealID NUMBER;  	-- здесь определяется ID сделки списания (если есть пара зачисления/списания, фактически перевод между суб-счетами)
    x_ConvFIID NUMBER;  	-- ФИ конвертации
    x_ConvPartyID NUMBER;  	-- ID клиента по конвертации
    x_FIID NUMBER;
    x_PartyID NUMBER;
    x_DealID NUMBER;
    x_DealDate DATE;  		-- дата сделки замещения
    x_Numerator NUMBER;
    x_Denominator NUMBER;
    x_SumPrecision NUMBER;
    x_Rows NUMBER;
    x_Cur NUMBER;
    x_IsDepo NUMBER;
    x_BuyDealID NUMBER;
    x_Amount NUMBER;
    x_CntNdfl NUMBER;
    x_SumNdfl NUMBER;
    x_ConvSumID NUMBER := 0;
  BEGIN
    x_Sql := GetInsMatchTicksTmpSql();		
    WHILE( GetConvStatus0(x_BuySumID, x_ParentID, x_SaleDealDate, x_SaleDealTime, x_SaleDealID, x_Cur, x_IsDepo, x_BuyDealID, x_Amount) = 1 ) LOOP
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'x_ParentID: '||x_ParentID||', x_BuySumID: '||x_BuySumID||', x_BuyDealID: '||x_BuyDealID||', x_Amount: '||x_Amount ) ;
      END IF;
      -- Получаем инфу о лоте конвертации
      GetConvInfo(x_BuySumID, x_ConvFIID, x_ConvPartyID, x_Numerator, x_Denominator, x_SumPrecision);
      g_Numerator := g_Numerator * x_Numerator;
      g_Denominator := g_Denominator * x_Denominator;
      x_Numerator := g_Numerator;
      x_Denominator := g_Denominator;
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'x_ConvFIID: '||x_ConvFIID||', x_ConvPartyID: '||x_ConvPartyID||', x_IsDepo: '||x_IsDepo ) ;
        it_log.log( p_msg => 'x_Numerator: '||x_Numerator||', x_Denominator: '||x_Denominator||', x_SumPrecision: '||x_SumPrecision ) ;
      END IF;
      -- AVT_BOSS-1757 Обработка сделок зачисления НДФЛ перенесена в ProcessDepo()
      -- по DEF-107734 возвращена обратно
      IF(x_IsDepo = 1) THEN 
        x_CntNdfl := 0;
        x_SumNdfl := 0;
        ProcessNdfl (
           x_BuyDealID, x_ConvPartyID, x_ConvFIID
           , ((x_Amount * x_Denominator) / x_Numerator)
           , x_SaleDealDate, x_SaleDealTime, x_SaleDealID, x_Cur
           , x_CntNdfl, x_SumNdfl
        );
        UpdateNdflConv( x_ConvPartyID, x_ConvFIID, x_Numerator, x_Denominator, x_SumPrecision );
      END IF;
      -- Смотрим лоты конвертации
      FOR j IN (
         SELECT 
           bl.t_sumid, bl.t_DealID, bt.t_DealDate, bt.t_DealTime, lnk.t_amount t_Principal, bt.t_pfi AS t_FIID
           , bt.t_clientID AS t_PartyID, bt.t_BOfficeKind AS t_BuyDocKind
           FROM dpmwrtlnk_dbt lnk  -- связка между лотом-конвертации и лотом-покупки
           INNER JOIN dpmwrtsum_dbt bl ON (bl.t_sumid = lnk.t_buyid)     -- лот-покупки
           INNER JOIN ddl_tick_dbt bt ON (bt.t_dealid = bl.t_DealID) -- ценовые условия покупки
           INNER JOIN ddl_leg_dbt leg ON (leg.t_dealid = bt.t_dealid and leg.t_LegKind = 0 AND leg.t_LegID = 0) -- ценовые условия покупки
           WHERE lnk.t_saleid = x_ParentID
      ) LOOP
         x_OutDealID := GetOutDealID( j.t_dealid );
         IF(g_DebugFlag = 1) THEN
           it_log.log( p_msg => 'j.t_dealid: '||j.t_dealid||', x_OutDealID: '||x_OutDealID ) ;
         END IF;
         -- Пропуск сделки j.t_dealid = x_BuyDealID удален по кейсу AVT_BOSS-573_AVT_BOSS-1309
         IF(x_OutDealID IS NULL) THEN 
           -- это не cделка перевода между суб-договорами,
           -- отправляем в таблицу, как есть
           x_DealID := RSI_SUMCONFEXP.GetSubstDeal( j.t_dealid );
           IF( x_DealID <> -1 ) THEN
             -- сделка замещения
             GetSubstInfo(x_DealID, x_FIID, x_PartyID, x_DealDate);
           ELSE
             -- не сделка замещения
             x_FIID := j.t_FIID;
             x_PartyID := j.t_PartyID;
             x_DealDate := j.t_dealdate;
           END IF;
           EXECUTE IMMEDIATE x_Sql USING 
              x_SaleDealDate, x_SaleDealTime, x_SaleDealID
              , j.t_sumid, j.t_dealdate, j.t_dealtime, j.t_dealid, j.t_principal
              , 0, RSI_SUMCONFEXP.IsDepoLot( j.t_sumid ), RSI_SUMCONFEXP.GetNKD( j.t_dealid )
              , x_FIID, x_PartyID, x_DealID
              , 0 -- t_SubstStatus
              , RSI_SUMCONFEXP.GetParentID( j.t_sumid )
              , 0 -- t_ConvStatus
              , 1 -- IsConv
              , x_Numerator, x_Denominator, x_SumPrecision
              , j.t_principal, j.t_BuyDocKind
              , g_Cur
              , 0
              , x_ConvSumID 
           ;
         ELSE
           -- это сделка перевода между суб-договорами, ищем исходные лоты приобретения
           FOR k IN (
              SELECT 
                b.t_sumid, b.t_dealid, t.t_dealdate, t.t_dealtime, l.t_amount AS t_principal, t.t_pfi AS t_FIID
                , t.t_clientID AS t_PartyID, t.t_BOfficeKind AS t_BuyDocKind
                FROM dpmwrtsum_dbt s 
                JOIN dpmwrtlnk_dbt l ON (l.t_saleid = s.t_sumID) 
                JOIN dpmwrtsum_dbt b ON (b.t_sumid = l.t_buyID)
                JOIN ddl_tick_dbt t ON (t.t_dealid = b.t_dealid) 
                JOIN ddl_leg_dbt e ON (e.t_dealid = t.t_dealid AND e.t_legkind = 0 AND e.t_legid  = 0)
                WHERE s.t_dealid = x_OutDealID
           ) LOOP
              -- направляем в таблицу найденные лоты (которые, фактически, подменяют сделку перевода между суб-счетами)
              x_DealID := RSI_SUMCONFEXP.GetSubstDeal( k.t_dealid );
              IF( x_DealID <> -1 ) THEN
                -- сделка замещения
                GetSubstInfo(x_DealID, x_FIID, x_PartyID, x_DealDate);
              ELSE
                -- не сделка замещения
                x_FIID := k.t_FIID;
                x_PartyID := k.t_PartyID;
                x_DealDate := k.t_dealdate;
              END IF;
              EXECUTE IMMEDIATE x_Sql USING 
                 x_SaleDealDate, x_SaleDealTime, x_SaleDealID
                 , k.t_sumid, k.t_dealdate, k.t_dealtime, k.t_dealid, k.t_principal
                 , 0, RSI_SUMCONFEXP.IsDepoLot( k.t_sumid ), RSI_SUMCONFEXP.GetNKD( k.t_dealid )
                 , x_FIID, x_PartyID, x_DealID
                 , 0 -- t_SubstStatus
                 , RSI_SUMCONFEXP.GetParentID( k.t_sumid ) -- t_ConvStatus
                 , 0
                 , 1  -- IsConv
                 , x_Numerator, x_Denominator, x_SumPrecision
                 , k.t_principal, k.t_BuyDocKind
                 , g_Cur
                 , 0
                 , x_ConvSumID
              ;
           END LOOP;
         END IF;
      END LOOP;
      -- Помечаем лот конвертации, как обработанный
      UPDATE dmatchticks_tmp m 
        SET m.t_convStatus = 1
        WHERE m.t_parentID = x_ParentID;
      x_Rows := SQL%ROWCOUNT;
      COMMIT;
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'UPDATE x_ParentID: '||x_ParentID||', x_Rows: '||x_Rows  ) ;
      END IF;
    END LOOP;
  END ProcessConv;

  /** 
   @brief    Процедура переноса НКД со сделок "зачисления ДЕПО" (если они есть с расчитанным НКД)
             на сделки "зачисления НДФЛ" (если они есть с нерасчитанным НКД).
             Всё из-за кривизны данных, сделки "зачисления ДЕПО" не обрабатываются (но НКД у них может быть),
             а сделки "зачисления НДФЛ", наоборот, обрабатываются (но НКД у них может не быть).
             Поэтому, после сбора данных, нужно пробежаться по сделкам "зачисления ДЕПО" и попытаться перенести НКД с них
             на сделки "зачисления НДФЛ".
  */
  PROCEDURE MoveNKD
  AS
  BEGIN
    FOR i IN (SELECT r.t_FIID, r.t_NKD FROM dmatchticks_tmp r WHERE r.t_IsDepo = 1 and r.t_NKD > 0) LOOP
       UPDATE dmatchticks_tmp t SET t.t_NKD = i.t_NKD WHERE t.t_IsNdfl = 1 and t.t_NKD = 0 and t.t_FIID = i.t_FIID AND rownum = 1;
    END LOOP;
  END MoveNKD;

  /** 
   @brief    DEF-81824
             Процедура простановки значение t_IsSkip.
             Проставляется для сделок "Зачисления ДЕПО" при наличии сделок "Зачисления НДФЛ". 
             (таким образом эти сделки "Зачисления ДЕПО" в отчет не попадут).
             Но попадут в отчет сделки "Зачисления ДЕПО", у которых нет сделок "Зачисления НДФЛ"
             DEF-88167
             Необходимо производить подобную манипуляцию по отдельным бумагам
  */
  PROCEDURE ProcessIsSkip
  AS
    x_IsNdfl NUMBER;
  BEGIN
    FOR i IN (SELECT t_FIID FROM dmatchticks_tmp t WHERE t.t_IsNdfl = 1 group by t_FIID) LOOP
      UPDATE dmatchticks_tmp t SET t.t_IsSkip = 1 WHERE t.t_IsDepo = 1 AND t.t_FIID = i.t_FIID;
    END LOOP;
    commit;
  END ProcessIsSkip;

  /** 
   @brief    Процедура заполнения таблицы dmatchticks_tmp
             В качестве параметра получает курсор с данными.
             Курсор просматривается и, при наличии, переводы между суб-договорами заменяются на лоты приобретения
  */
  PROCEDURE FillDmatchticksTmp ( p_Cursor IN SYS_REFCURSOR, p_PartyID IN NUMBER, p_FIID IN NUMBER )
  AS
    x_MatchTicksTmpSql CLOB;
    x_Rec t_MatchTickRec;
    x_OutDealID NUMBER;  	-- здесь определяется ID сделки списания (если есть пара зачисления/списания, фактически перевод между суб-счетами)
    x_SubstID NUMBER;  		-- здесь определяется ID сделки замещения
    x_Processed NUMBER;
    x_HasSubst NUMBER;		-- если 1, то есть лоты замещения
    x_HasConv NUMBER;		-- если 1, то есть лоты конвертации
    x_HasDepo NUMBER;		-- если 1, то есть сделки "зачисления ДЕПО"
    x_I NUMBER := 0;
    x_ConvSumID NUMBER;
    x_ParentID NUMBER;
  BEGIN

    x_MatchTicksTmpSql := GetInsMatchTicksTmpSql();		

    -- AVT_BOSS-573_AVT_BOSS-1309 Учет нескольких конвертаций
    g_Numerator := 1;
    g_Denominator := 1;
    g_Cur := p_FIID;
    g_DepoProcessed := 0;

    LOOP
      FETCH p_Cursor INTO x_Rec;
      EXIT WHEN p_Cursor%NOTFOUND;

      x_ConvSumID := 0;
      x_ParentID := 0;

      IF(x_I = 0) THEN
        -- Если в лотах отсутствуют "Зачисления НДФЛ", соберем их отдельно
        GetNdflTicks( x_Rec.t_saledealdate, x_Rec.t_saledealtime, x_Rec.t_saledealid, p_PartyID, p_FIID, p_FIID );
        x_I := 1;
      END IF;

      x_OutDealID := GetOutDealID( x_Rec.t_buydealid );
      IF(x_OutDealID IS NULL) THEN 
        -- это не cделка перевода между суб-договорами,
        -- отправляем в таблицу, как есть
        EXECUTE IMMEDIATE x_MatchTicksTmpSql USING 
           x_Rec.t_saledealdate, x_Rec.t_saledealtime, x_Rec.t_saledealid
           , x_Rec.t_buysumid, x_Rec.t_buydealdate, x_Rec.t_buydealtime, x_Rec.t_buydealid, x_Rec.t_amount
           , x_Rec.t_IsNdfl, x_Rec.t_IsDepo, x_Rec.t_NKD
           , p_FIID, p_PartyID, RSI_SUMCONFEXP.GetSubstDeal( x_Rec.t_buydealid ), 0
           , RSI_SUMCONFEXP.GetParentID( x_Rec.t_buysumid ), 0, 0, 1, 1, 6
           , x_Rec.t_Principal, x_Rec.t_BuyDocKind, g_Cur, 0, x_ConvSumID
        ;
      ELSE
        -- это сделка перевода между суб-договорами, ищем исходные лоты приобретения
        IF(g_DebugFlag = 1) THEN
          it_log.log( p_msg => 'x_Rec.t_buydealid: '||x_Rec.t_buydealid||', x_OutDealID: '||x_OutDealID ) ;
        END IF;
        IF(x_Rec.t_ParentID is not null) THEN
          x_ParentID := x_Rec.t_ParentID;
          x_ConvSumID := x_Rec.t_BuySumID;
        END IF;  
        FOR i IN (
           SELECT b.t_sumid, b.t_dealid, t.t_dealdate, t.t_dealtime, l.t_amount AS t_principal -- DEF-89639 исправление, было: e.t_principal
             FROM dpmwrtsum_dbt s 
             JOIN dpmwrtlnk_dbt l ON (l.t_saleid = s.t_sumID) 
             JOIN dpmwrtsum_dbt b ON (b.t_sumid = l.t_buyID)
             JOIN ddl_tick_dbt t ON (t.t_dealid = b.t_dealid) 
             JOIN ddl_leg_dbt e ON (e.t_dealid = t.t_dealid AND e.t_legkind = 0 AND e.t_legid  = 0)
             WHERE s.t_dealid = x_OutDealID
        ) LOOP
           -- направляем в таблицу найденные лоты (которые, фактически, подменяют сделку перевода между суб-счетами)
           EXECUTE IMMEDIATE x_MatchTicksTmpSql USING 
              x_Rec.t_saledealdate, x_Rec.t_saledealtime, x_Rec.t_saledealid
              , i.t_sumid, i.t_dealdate, i.t_dealtime, i.t_dealid, i.t_principal
              , 0, RSI_SUMCONFEXP.IsDepoLot( i.t_sumid ), RSI_SUMCONFEXP.GetNKD(i.t_dealid)
              , p_FIID, p_PartyID, RSI_SUMCONFEXP.GetSubstDeal( i.t_dealid ), 0
              , case when x_Rec.t_ParentID is not null then x_Rec.t_ParentID else RSI_SUMCONFEXP.GetParentID( i.t_sumid ) end
              , 0, 0, 1, 1, 6
              , x_Rec.t_Principal, x_Rec.t_BuyDocKind, g_Cur, 0, x_ConvSumID
           ;
        END LOOP;
      END IF;
    END LOOP;
    CLOSE p_Cursor;

    -- Обработка сделок замещений или конвертаций 
    -- (возможно несколько проходов, так как при получении лотов по сделке замещения (или конвертации)
    -- может оказаться так, что сделка приобретения окажется сделкой замещения (или конвертации), 
    -- и ее тогда придется обрабатывать дополнительно путем получения лотов для нее)
    x_I := 0;
    LOOP
      x_HasSubst := ExistSubst();
      x_HasConv := ExistConv();
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'x_I: '||x_I||', x_HasSubst: '||x_HasSubst||', x_HasConv: '||x_HasConv ) ;
      END IF;
      IF( (x_HasSubst = 0) AND (x_HasConv = 0)) THEN
        -- выходим из цикла, если нет необработанных лотов замещения или конвертации
        EXIT;
      END IF;
      IF(x_I > 10) THEN
        EXIT;
      END IF;
      IF(x_HasSubst <> 0) THEN
        ProcessSubst();
      END IF;
      IF(x_HasConv <> 0) THEN
        ProcessConv();
      END IF;
      x_I := x_I + 1;
    END LOOP;

    -- DEF-98100 Обработка сделок "зачисления ДЕПО" 
    x_I := 0;
    LOOP
      x_HasDepo := ExistDepo();
      IF(x_HasDepo = 0) THEN
        -- выходим из цикла, если нет необработанных сделок ДЕПО
        EXIT;
      END IF;
      IF(x_I > 10) THEN
        EXIT;
      END IF;
      IF(x_HasDepo <> 0) THEN
        ProcessDepo();			
      END IF;
      x_I := x_I + 1;
    END LOOP;

    -- DEF-81824 Обработка признака t_IsSkip
--    ProcessIsSkip(); -- После реализации DEF-98100 не нужен, так как обработка производится внутри ProcessDepo()

    -- Процедура переноса НКД
    MoveNKD();

  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END FillDmatchticksTmp;

  /** 
   @brief    Заполняет таблицу dmatchticks_tmp 
             данными о сделках 'Зачисления НДФЛ' (инфа о которых отсутствует в лотах)
             Вряд ли сделка 'Зачисления НДФЛ' является сделкой замещения
  */
  PROCEDURE GetNdflTicks( p_SaleDealDate IN DATE, p_SaleDealTime IN DATE, p_SaleDealID IN NUMBER, p_PartyID IN NUMBER, p_FIID IN NUMBER, p_Cur IN NUMBER )
  AS
    x_Rows NUMBER;
  BEGIN
    RETURN ; -- заглушка для DEF-98100
    IF(not x_ViewedDepo.exists(p_FIID) ) THEN
      x_ViewedDepo(p_FIID) := 1; -- DEF-84364, помечаем валюту, как просмотренную, во избежание дублирования
      INSERT INTO dmatchticks_tmp r (
         r.t_saledealdate, r.t_saledealtime, r.t_saledealid
         , r.t_buysumid, r.t_buydealdate, r.t_buydealtime, r.t_buydealid, r.t_amount
         , r.t_IsNdfl, r.t_IsDepo, r.t_NKD
         , r.t_FIID, r.t_PartyID, r.t_SubstID, r.t_SubstStatus
         , r.t_ParentID, r.t_ConvStatus, r.t_IsConv, r.t_Numerator, r.t_Denominator, r.t_SumPrecision
         , r.t_Principal, r.t_BuyDocKind, r.t_Cur, r.t_DepoStatus
      )
      SELECT 
        p_SaleDealDate, p_SaleDealTime, p_SaleDealID 
        , -1 AS t_BuySumID, t.t_dealdate AS t_BuyDealDate, t.t_dealtime AS t_BuyDealTime, t.t_dealid AS t_BuyDealID, l.t_principal AS t_amount
        , 1 AS t_IsNdfl, 0 AS t_IsDepo
        , RSI_SUMCONFEXP.GetNKD(t.t_dealid) AS t_NKD
        , p_FIID, p_PartyID, -1 AS t_SubstID, 1 AS t_SubstStatus
        , -1 AS t_ParentID, 1 AS t_ConvStatus, 0 AS t_IsConv
        , 1 AS t_Numerator, 1 AS t_Denominator, 6 AS t_SumPrecision
        , l.t_Principal AS t_Principal
        , t.t_BOfficeKind AS t_BuyDocKind, g_Cur, 0
      FROM ddl_tick_dbt t
      JOIN ddl_leg_dbt l ON (t.t_dealid = l.t_Dealid and l.t_legkind = 0 and l.t_legid = 0)
      JOIN dobjatcor_Dbt a ON (A.T_GROUPID = 210 and A.T_OBJECTTYPE = 101 and a.t_object = lpad(t.t_dealid, 34,'0'))
      LEFT JOIN dpmwrtsum_dbt s ON (s.t_dealid = t.t_dealid)
      WHERE t.t_clientid = p_PartyID and t.t_pfi = p_FIID
      AND s.t_dealid is null
      ;
      x_Rows := SQL%ROWCOUNT;
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'p_PartyID: '||p_PartyID||', p_FIID: '||p_FIID||', x_Rows: '||x_Rows ) ;
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END GetNdflTicks;

  /** 
   @brief    Заполняет таблицу dmatchticks_tmp (для режима 'ЦБ на дату'), 
             с информацией о сделках покупках-продажи, которая является входной информацией для работы процедуры CreateRepData()
  */
  PROCEDURE GetBuyTicks (
     p_OnDate IN DATE, p_PartyID IN NUMBER, p_FIID IN NUMBER
  )
  AS
    x_Sort NUMBER := 0;
    x_Sql CLOB;
    x_FiidList VARCHAR2(256);
    x_RepStartDate DATE;
    x_RepEndDate DATE;
    x_MaxChangeDate DATE;
    x_Cursor SYS_REFCURSOR;
  BEGIN
    x_MaxChangeDate := GetMaxChangeDate( p_FIID, p_PartyID );
    IF(p_OnDate > x_MaxChangeDate) THEN 
      -- можно обойтись без архива
      x_Sql := GetBuyTicksSql();		
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'GetBuyTicksSql(), p_Fiid: '||p_Fiid||', p_PartyID: '||p_PartyID, p_msg_clob => x_Sql ) ;
      END IF;
      OPEN x_Cursor FOR x_Sql USING p_Fiid, p_PartyID;
    ELSE
      -- используются архивные данные
      x_Sql := GetBuyTicksSql_bc();		
      IF(g_DebugFlag = 1) THEN
        it_log.log( 
            p_msg => 'GetBuyTicksSql_bc(), p_Fiid: '||p_Fiid||', p_PartyID: '||p_PartyID||', p_OnDate: '||p_OnDate
            , p_msg_clob => x_Sql 
        );
      END IF;
      OPEN x_Cursor FOR x_Sql USING p_Fiid, p_PartyID, p_OnDate;
    END IF;

    -- заполнение таблицы dmatchticks_tmp
    -- Выполняется отдельной процедурой, которой передается курсор, подготовленный выше, 
    -- В процессе заполнения таблицы, переводы между суб-договорами могут замениться на записи по лотам приобретения
    -- (собственно, процедура для этого и предназначена)
    FillDmatchticksTmp( x_Cursor, p_PartyID, p_FIID );

  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END GetBuyTicks;

  /** 
   @brief    Получение данных отчета для режима 'ЦБ на дату'
  */
  PROCEDURE CreateRepDataByContr_0_byFiidLots (
     p_GUID IN VARCHAR2
     , p_DlContrID IN NUMBER
     , p_OnDate IN DATE
     , p_FIID IN NUMBER
  )
  AS
    x_PartyID NUMBER;
  BEGIN
    
    IF p_DlContrID > 0 THEN 
       x_PartyID := GetPartyID( p_DlContrID );
       -- Формируем список сделок для работы 
       GetBuyTicks( p_OnDate, x_PartyID, p_FIID );
    END IF;

  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END CreateRepDataByContr_0_byFiidLots;

  /** 
   @brief    Получение данных отчета для режима 'ЦБ на дату'
  */
  PROCEDURE CreateRepDataByContr_0_byLots (
     p_GUID IN VARCHAR2
     , p_DlContrID IN NUMBER
     , p_OnDate IN DATE
     , p_FIID IN NUMBER
  )
  AS
    x_PartyID NUMBER;
  BEGIN
    
    IF p_DlContrID <= 0 THEN 
      RETURN; 
    END IF;

    SELECT sf.t_PartyID INTO x_PartyID
      FROM ddlcontr_dbt dlcontr, dsfcontr_dbt sf
     WHERE dlcontr.t_DlContrID = p_DlContrID
       AND sf.t_ID = dlcontr.t_SfContrID;

    FOR i IN (SELECT NVL(SUM(q.AccRest), 0) as SumAccRest, q.t_Currency AS fiid
                      FROM (SELECT ABS(rsb_account.restac(q1.t_Account, q1.t_Currency, p_OnDate, q1.t_Chapter, null)) as AccRest,
                                   q1.t_Currency
                              FROM (SELECT /*+ leading(mp) */ DISTINCT accd.t_Account, accd.t_Chapter, accd.t_Currency
                                      FROM dmcaccdoc_dbt accd, ddlcontrmp_dbt mp
                                     WHERE accd.t_CatID = 364 -- t_Code = 'ЦБ Клиента, ВУ'
                                       AND accd.t_Owner = x_PartyID
                                       AND accd.t_Currency = (CASE WHEN p_FIID > 0 THEN p_FIID ELSE accd.t_Currency END)
                                       AND accd.t_IsCommon = 'X'
                                       AND accd.t_ActivateDate <= p_OnDate
                                       AND (accd.t_DisablingDate = g_NullDate or accd.t_DisablingDate >= p_OnDate)
                                       AND mp.t_SfContrID = accd.t_ClientContrID
                                       AND mp.t_DlContrID = p_DlContrID
                                   ) q1
                           ) q
                     WHERE q.AccRest > 0
                    GROUP BY q.t_Currency
                   )
    LOOP
      
      CreateRepDataByContr_0_byFiidLots(p_GUID, p_DlContrID, p_OnDate, i.fiid);

    END LOOP;

  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END CreateRepDataByContr_0_byLots;

  /** 
   @brief    Получение данных отчета для режима 'ЦБ на дату'
  */
  PROCEDURE CreateRepDataByContr_0 ( p_GUID IN VARCHAR2, p_DlContrID IN NUMBER, p_OnDate IN DATE, p_FIID IN NUMBER, p_Cnt OUT NUMBER )
  AS
    x_PartyID NUMBER;
  BEGIN
     -- BOSS-2935_BOSS-4183, рубильник по отражению конвертаций в отчете
     IF(x_RegProcessConv) THEN
       -- отражать конвертации (работа через лоты)
       IF(g_DebugFlag = 1) THEN
         it_log.log( p_msg => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\КОНВЕР_В_СПРАВКЕ_ГОССЛ = yes' ) ;
       END IF;
       CreateRepDataByContr_0_byLots( p_GUID, p_DlContrID, p_OnDate, p_FIID );
       -- Запускаем отчет по списку
       x_PartyID := GetPartyID( p_DlContrID );
       CreateRepData ( p_GUID, p_DlContrID, x_PartyID, p_OnDate, p_Cnt );
     ELSE
       -- отражение конвертаций в выключенном состоянии
       -- работаем, как было реализовано раньше
       IF(g_DebugFlag = 1) THEN
         it_log.log( p_msg => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\КОНВЕР_В_СПРАВКЕ_ГОССЛ = no' ) ;
       END IF;
       CreateRepDataByContr_0_old( p_GUID, p_DlContrID, p_OnDate, p_FIID );
     END IF;
  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END CreateRepDataByContr_0;

  /** 
   @brief    Формирование списка сделок для работы  для режима 'Переведенные другому брокеру'
  */
  PROCEDURE GetMatchTicks (
     p_DlContrID IN NUMBER, p_OnDate IN DATE, p_CurDate IN DATE, p_PartyID IN NUMBER, p_FIID IN NUMBER
  )
  AS
    x_Sort NUMBER := 0;
    x_Sql CLOB;
    x_FiidList VARCHAR2(256);
    x_RepStartDate DATE;
    x_RepEndDate DATE;
    x_Cursor SYS_REFCURSOR;
  BEGIN
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'p_DlContrID: '||p_DlContrID||', p_OnDate='||p_OnDate||', p_CurDate='||p_CurDate||', p_PartyID='||p_PartyID||', p_FIID='||p_FIID ) ;
    END IF;
    x_FiidList := GetFiidList( p_FIID );

    IF((p_OnDate IS NULL) OR (p_OnDate = g_NullDate)) THEN  
      -- режим с не заданной датой отчета
      x_RepStartDate := g_NullDate;
      x_RepEndDate := p_CurDate;
    ELSE
      -- режим с заданной датой отчета
      x_RepStartDate := p_OnDate - 30;
      x_RepEndDate := p_OnDate;
    END IF;

    -- строим SQL-выражение для получения сделок в режиме 'Переведенные другому брокеру'
    x_Sql := GetMatchTicksSql( x_FiidList );		

    -- проход по курсору, заполнение массива
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'Start: '||x_Sort||', startDate='||x_RepStartDate||', endDate='||x_RepEndDate, p_msg_clob => x_Sql ) ;
      it_log.log( p_msg => 'p_PartyID='||p_PartyID||', p_DlContrID='||p_DlContrID) ;
    END IF;
    OPEN x_Cursor FOR x_Sql USING x_RepStartDate, x_RepEndDate, p_PartyID, p_DlContrID, RSB_SECUR.DL_AVRWRT, RSB_SECUR.OBJTYPE_SECDEAL;

    -- заполнение таблицы dmatchticks_tmp
    -- Выполняется отдельной процедурой, которой передается курсор, подготовленный выше, 
    -- В процессе заполнения таблицы, переводы между суб-договорами могут замениться на записи по лотам приобретения
    -- (собственно, процедура для этого и предназначена)
    FillDmatchticksTmp( x_Cursor, p_PartyID, p_FIID );

  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END GetMatchTicks;


  /** 
   @brief    Получение данных отчета для режима 'Переведенные другому брокеру'
  */
  PROCEDURE CreateRepDataByContr_1_fiid (
     p_GUID IN VARCHAR2
     , p_DlContrID IN NUMBER
     , p_OnDate IN DATE
     , p_CurDate IN DATE
     , p_FIID IN NUMBER
  )
  AS
    x_PartyID NUMBER;
  BEGIN
    IF p_DlContrID > 0 THEN 
       x_PartyID := GetPartyID( p_DlContrID );
       -- Формируем список сделок для работы 
       GetMatchTicks( p_DlContrID, p_OnDate, p_CurDate, x_PartyID, p_FIID );
    END IF;
  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END CreateRepDataByContr_1_fiid;


  /** 
   @brief    Получение данных отчета для режима 'Переведенные другому брокеру'
  */
  PROCEDURE CreateRepDataByContr_1 (
     p_GUID IN VARCHAR2
     , p_DlContrID IN NUMBER
     , p_OnDate IN DATE
     , p_CurDate IN DATE
     , p_FIID IN NUMBER
     , p_Cnt OUT NUMBER
  )
  AS
    x_PartyID NUMBER;
    x_OnDate DATE := NVL(p_OnDate, g_NullDate);
    x_Cursor SYS_REFCURSOR;
    x_FIID NUMBER;
    x_Sql CLOB;
  BEGIN
    
    IF p_DlContrID <= 0 THEN 
      RETURN; 
    END IF;

    -- Если валюта передана явно, то вызываем функцию
    IF ( p_FIID > 0) THEN
      CreateRepDataByContr_1_fiid(p_GUID, p_DlContrID, p_OnDate, p_CurDate, p_FIID);
    ELSE
      -- Если валюта не определена, определяем самостоятельно
      SELECT sf.t_PartyID INTO x_PartyID
        FROM ddlcontr_dbt dlcontr, dsfcontr_dbt sf
       WHERE dlcontr.t_DlContrID = p_DlContrID
         AND sf.t_ID = dlcontr.t_SfContrID;

      -- DEF-73307 Если дата не задана
      -- DEF-79765 Изменен запрос по определению FIID
      IF(x_OnDate = g_NullDate) THEN
        x_Sql := q'[
           -- входные параметры
           WITH prm AS (
             SELECT 
               :t_OnDate AS t_OnDate
               , :t_PartyID AS t_PartyID
               , :t_FIID AS t_FIID
               , :t_DlContrID AS t_DlContrID
               , :t_AvrWrt AS t_AvrWrt                        	-- RSB_SECUR.DL_AVRWRT
               , :t_ObjTypeSecdeal AS t_ObjTypeSecdeal              -- RSB_SECUR.OBJTYPE_SECDEAL
             FROM 
               dual
           )
           SELECT DISTINCT tk.t_PFI
             FROM prm, (SELECT t_Kind_Operation 
                     FROM doprkoper_dbt, prm
                    WHERE t_DocKind = prm.t_AvrWrt
                      AND Rsb_Secur.IsAvrWrtOut(rsb_secur.get_OperationGroup(t_SysTypes)) = 1) opr, ddl_tick_dbt tk, ddlcontrmp_dbt mp
            WHERE tk.t_BOfficeKind = prm.t_AvrWrt
              AND tk.t_DealType = opr.t_Kind_Operation
              AND tk.t_ClientID > 0
              AND tk.t_ClientID = prm.t_PartyID
              AND tk.t_PFI = (CASE WHEN prm.t_FIID > 0 THEN prm.t_FIID ELSE tk.t_PFI END)
              AND tk.t_DealDate <= prm.t_OnDate
              AND RSB_SECUR.GetMainObjAttrNoDate(prm.t_ObjTypeSecdeal, LPAD(tk.t_DealID, 34, '0'), 111 /*Внешняя операция*/) = 2 /*Да*/
              AND tk.t_DealStatus = 20 --Закрыта
              AND mp.t_SfContrID = tk.t_ClientContrID
              AND mp.t_DlContrID = prm.t_DlContrID 
        ]';
        IF(g_DebugFlag = 1) THEN
          it_log.log( p_msg => 'Дата не задана '||x_OnDate, p_msg_clob => x_Sql ) ;
        END IF;
        OPEN x_Cursor FOR x_Sql USING p_CurDate, x_PartyID, p_FIID, p_DlContrID, RSB_SECUR.DL_AVRWRT, RSB_SECUR.OBJTYPE_SECDEAL;
      ELSE
        x_Sql := q'[
           -- входные параметры
           WITH prm AS (
             SELECT 
               :t_OnDate AS t_OnDate
               , :t_PartyID AS t_PartyID
               , :t_FIID AS t_FIID
               , :t_DlContrID AS t_DlContrID
               , :t_AvrWrt AS t_AvrWrt                        	-- RSB_SECUR.DL_AVRWRT
               , :t_ObjTypeSecdeal AS t_ObjTypeSecdeal              -- RSB_SECUR.OBJTYPE_SECDEAL
             FROM 
               dual
           )
           SELECT DISTINCT tk.t_PFI
             FROM prm, (SELECT t_Kind_Operation 
                     FROM doprkoper_dbt, prm
                    WHERE t_DocKind = prm.t_AvrWrt
                      AND Rsb_Secur.IsAvrWrtOut(rsb_secur.get_OperationGroup(t_SysTypes)) = 1) opr, ddl_tick_dbt tk, ddlcontrmp_dbt mp
            WHERE tk.t_BOfficeKind = prm.t_AvrWrt
              AND tk.t_DealType = opr.t_Kind_Operation
              AND tk.t_ClientID > 0
              AND tk.t_ClientID = prm.t_PartyID
              AND tk.t_PFI = (CASE WHEN prm.t_FIID > 0 THEN prm.t_FIID ELSE tk.t_PFI END)
              AND tk.t_DealDate <= prm.t_OnDate
              AND tk.t_DealDate >= prm.t_OnDate-30
              AND RSB_SECUR.GetMainObjAttrNoDate(prm.t_ObjTypeSecdeal, LPAD(tk.t_DealID, 34, '0'), 111 /*Внешняя операция*/) = 2 /*Да*/
              AND tk.t_DealStatus = 20 --Закрыта
              AND mp.t_SfContrID = tk.t_ClientContrID
              AND mp.t_DlContrID = prm.t_DlContrID 
        ]';
        IF(g_DebugFlag = 1) THEN
          it_log.log( p_msg => 'Дата задана '||x_OnDate, p_msg_clob => x_Sql ) ;
        END IF;
        OPEN x_Cursor FOR x_Sql USING x_OnDate, x_PartyID, p_FIID, p_DlContrID, RSB_SECUR.DL_AVRWRT, RSB_SECUR.OBJTYPE_SECDEAL;
      END IF;

      LOOP
        FETCH x_Cursor INTO x_FIID;
        EXIT WHEN x_Cursor%NOTFOUND;
        CreateRepDataByContr_1_fiid(p_GUID, p_DlContrID, p_OnDate, p_CurDate, x_FIID);
      END LOOP;
      CLOSE x_Cursor;
    END IF;

    CreateRepData ( p_GUID, p_DlContrID, x_PartyID, p_OnDate, p_Cnt );

  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;
  END CreateRepDataByContr_1;


  /** 
   @brief    Получение данных для отчета по договору
  */
  PROCEDURE CreateSumConfirmExpRepDataByContr (
      p_DlContrID IN NUMBER
      , p_DLContrID_2 IN NUMBER
      , p_GUID IN VARCHAR2
      , p_OnDate IN DATE
      , p_CurDate IN DATE
      , p_LaunchMode IN NUMBER
      , p_FIID IN NUMBER
      , p_Cnt OUT NUMBER
  )
  IS
    x_clob clob;
    x_PartyID NUMBER;
    x_OnDate DATE := NVL(p_OnDate, g_NullDate);
  BEGIN
    x_clob := 
        to_clob('p_GUID: ['||p_GUID||']'||chr(13)||chr(10))
        || to_clob('p_DlContrID: ['||to_char(p_DlContrID)||']'||chr(13)||chr(10))
        || to_clob('x_OnDate: ['||to_char(x_OnDate, 'yyyy-mm-dd')||']'||chr(13)||chr(10))
        || to_clob('p_CurDate: ['||to_char(p_CurDate, 'yyyy-mm-dd')||']'||chr(13)||chr(10))
        || to_clob('p_LaunchMode: ['||to_char(p_LaunchMode)||']'||chr(13)||chr(10))
        || to_clob('p_FIID: ['||to_char(p_FIID)||']'||chr(13)||chr(10))
    ;
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'Start: p_DlContrID: '||p_DlContrID, p_msg_clob => x_clob ) ;
    END IF;

    IF p_LaunchMode = 0 THEN --ЦБ на дату

      CreateRepDataByContr_0(p_GUID, p_DlContrID, x_OnDate, p_FIID, p_Cnt);

    ELSIF p_LaunchMode = 1 THEN --Переведенные другому брокеру

      CreateRepDataByContr_1(p_GUID, p_DlContrID, x_OnDate, p_CurDate, p_FIID, p_Cnt);

    END IF;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'End: p_DlContrID: '||p_DlContrID||', p_Cnt: '||p_Cnt ) ;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN 
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Err: '||SQLERRM ) ;
      END IF;

  END CreateSumConfirmExpRepDataByContr;

  /**
   @brief    Генерация чанка (задания) для itt_parallel_exec.
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_DlContrID     	ID договора брокерского обслуживания
   @param[in]  p_FIID     	ID финансового инструмента
   @param[in]  p_LaunchMode    	режим запуска отчета
   @param[in]  p_OnDate    	дата отчета
   @param[in]  p_CurDate    	дата опер.дня
   @return			Количество сгенеренных заданий
  */
  FUNCTION GenChunk ( p_CalcID IN NUMBER, p_DlContrID IN NUMBER, p_FIID IN NUMBER, p_LaunchMode IN NUMBER, p_OnDate IN DATE, p_CurDate IN DATE ) RETURN NUMBER
  IS
    x_SqlIns VARCHAR2(1000);
  begin
    x_SqlIns := q'[INSERT INTO itt_parallel_exec ( calc_id, num01, num02, num03, dat01, dat02 ) VALUES ( :1, :2, :3, :4, :5, :6 )]';
    EXECUTE IMMEDIATE x_SqlIns USING p_CalcID, p_DlContrID, p_FIID, p_LaunchMode, p_OnDate, p_CurDate;
    RETURN SQL%ROWCOUNT;
  END GenChunk;

  /**
   @brief    Генерация чанка (задания) для itt_parallel_exec.
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ClientID     	ID клиента
   @param[in]  p_FIID     	ID финансового инструмента
   @param[in]  p_LaunchMode    	режим запуска отчета
   @param[in]  p_OnDate    	дата отчета
   @param[in]  p_CurDate    	дата опер.дня
   @return			Количество сгенеренных заданий
  */
  FUNCTION GenClientChunk ( p_CalcID IN NUMBER, p_ClientID IN NUMBER, p_FIID IN NUMBER, p_LaunchMode IN NUMBER, p_OnDate IN DATE, p_CurDate IN DATE ) RETURN NUMBER
  IS
    x_SqlIns VARCHAR2(1000);
    x_Cnt NUMBER := 0;
  begin
    for i in (
      select distinct t_dlcontrid
        from dsfcontr_dbt sf
        inner join ddlcontrmp_dbt mp on (mp.t_sfcontrid = sf.t_id)
        where sf.t_partyid = p_ClientID
        and (mp.t_mpclosedate = to_date('1-1-0001', 'dd-mm-yyyy')
          or mp.t_mpclosedate < p_OnDate )
    ) loop
       x_Cnt := x_Cnt + GenChunk ( p_CalcID, i.t_dlcontrid, p_FIID, p_LaunchMode, p_OnDate, p_CurDate );
    end loop;
    RETURN x_Cnt;
  END GenClientChunk;

  /**
   @brief    Функция для получения row_id задания
   @param[in]	p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[out]  p_DlContrID    	ID договора брокерского обслуживания
   @param[out]  p_FIID     	ID финансового инструмента
   @param[out]  p_LaunchMode   	режим запуска отчета
   @param[out]  p_OnDate    	дата отчета
   @param[out]  p_CurDate    	дата опер.дня
  */
  FUNCTION GetRowID (
     p_CalcID IN varchar2
     , p_DlContrID OUT NUMBER
     , p_FIID OUT NUMBER
     , p_LaunchMode OUT NUMBER
     , p_OnDate OUT DATE
     , p_CurDate OUT DATE
  ) 
  RETURN number
  IS
    x_RowID number;
    x_Ret NUMBER := 0;
    x_Sql CLOB;
    pragma autonomous_transaction;
  BEGIN
    x_Sql := 'UPDATE itt_parallel_exec partition (p'||p_CalcID||') r '
             ||' SET r.str01 = ''P'', r.dat03 = SYSDATE '
             ||' WHERE r.str01 is null AND rownum = 1 '
             ||' RETURNING r.row_id, num01, num02, num03, dat01, dat02 '
             ||' INTO :p_RowID, :p_DlContrID, :p_FIID, :p_LaunchMode, :p_OnDate, :p_CurDate '
    ;
    EXECUTE IMMEDIATE x_Sql RETURNING INTO x_RowID, p_DlContrID, p_FIID, p_LaunchMode, p_OnDate, p_CurDate;
    IF( SQL%ROWCOUNT <> 1) THEN
      x_RowID := NULL;
    ELSE
      IF(p_OnDate IS NULL) THEN
        p_OnDate := g_NullDate;
      END IF;
    END IF;
    COMMIT;
    RETURN x_RowID;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
      RETURN NULL;
  END GetRowID;

  /**
   @brief    отметка о завершении обработки задания
   @param[in]  p_RowID     	ID задания
   @param[in]  p_GUID     	GUID отчета
   @param[in]  p_Cnt     	кол-во строк, обработанных по заданию
  */
  PROCEDURE EndProcess ( p_RowID IN number, p_GUID IN varchar2, p_Cnt IN number )
  IS
    pragma autonomous_transaction;
  BEGIN
    UPDATE itt_parallel_exec r 
      SET r.str01 = 'S', r.str02 = p_GUID, r.dat04 = SYSDATE, r.num04 = p_Cnt
      WHERE r.row_id = p_RowID
    ;
    COMMIT;
  EXCEPTION
    WHEN others THEN
      ROLLBACK;
  END;

  /**
   @brief    Нитка параллельной обработки заданий.
   @param[in]  p_ParaID     	номер процесса
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_Limit     	ограничитель итераций для какждого потока, если 0 -- выполняется всё
   @param[in]  p_GUID     	GUID отчета
  */
  PROCEDURE ExecParallelProc ( p_ParaID IN NUMBER, p_CalcID IN VARCHAR2, p_Limit IN NUMBER, p_GUID IN VARCHAR2 )
  IS
    x_Prefix VARCHAR2(64) := '('||to_char(p_ParaID)||', SessionID: '||TO_CHAR (USERENV ('sessionid'))||')';
    x_StartTime pls_integer;
    x_RowID NUMBER;
    x_Count NUMBER := 0;
    x_DlContrID NUMBER;
    x_FIID NUMBER;
    x_LaunchMode NUMBER;
    x_OnDate DATE;
    x_CurDate DATE;
    x_Cnt NUMBER;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => x_Prefix||', CalcID: '||p_CalcID||', p_Limit: '||p_Limit, p_msg_type => it_log.c_msg_type__debug );
    END IF;

    LOOP
       x_RowID := RSI_SUMCONFEXP.GetRowID(					-- получаем ДБО для обработки
           p_CalcID, x_DlContrID, x_FIID
           , x_LaunchMode, x_OnDate, x_CurDate
       );   				
       EXIT WHEN x_RowID IS NULL;          		            		-- завершаем обработку, если задания закончились
       IF(g_DebugFlag = 1) THEN
         it_log.log( 
           p_msg => to_char(p_ParaID)||': x_RowID: '||x_RowID||', x_DlContrID: '||x_DlContrID||', x_FIID: '||x_FIID
                    ||', x_LaunchMode: '||x_LaunchMode||', x_OnDate: '||x_OnDate||', x_CurDate: '||x_CurDate
                    ||', p_GUID: '||p_GUID
           , p_msg_type => it_log.c_msg_type__debug 
         );
       END IF;
       x_ViewedSubst.DELETE;
       x_ViewedDepo.DELETE;
       DELETE FROM dmatchticks_tmp;
       COMMIT;  
       x_Cnt := 0;
       RSI_SUMCONFEXP.CreateSumConfirmExpRepDataByContr(
         x_DlContrID, x_DlContrID, p_GUID, x_OnDate, x_CurDate, x_LaunchMode, x_FIID, x_Cnt
       );
       RSI_SUMCONFEXP.EndProcess ( x_RowID, p_GUID, x_Cnt );			-- отметка о завершении обработки партиции
       IF(g_DebugFlag = 1) THEN
         it_log.log( 
           p_msg => to_char(p_ParaID)||': End: '||x_RowID||', x_Cnt: '||x_Cnt, p_msg_type => it_log.c_msg_type__debug 
         );
       END IF;
       COMMIT;
       x_Count := x_Count + 1;
       IF(p_Limit <> 0 AND x_Count >= p_Limit) THEN
         EXIT;
       END IF;
    END LOOP;

    -- Сообщение о завершении процедуры
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => x_Prefix||', End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );
    END IF;

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log( p_msg => x_Prefix||', Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
      it_error.clear_error_stack;
      RAISE;

  END ExecParallelProc;

  /**
   @brief    Процедура параллельного получения данных для отчета.
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ParaLevel     	кол-во параллельных процессов
   @param[in]  p_Limit     	ограничитель итераций для какждого потока, если 0 -- выполняется всё
   @param[in]  p_GUID     	GUID отчета
  */
  PROCEDURE ExecParallel ( p_CalcID IN varchar2, p_ParaLevel IN NUMBER, p_Limit IN number, p_GUID IN varchar2 )
  IS
    x_CalcID VARCHAR2(64);
    x_ChunkSql VARCHAR2(2000);
    x_SqlStmt VARCHAR2(2000);
    x_StartTime pls_integer;
  BEGIN
    x_StartTime := dbms_utility.get_time;

    -- Сообщение о начале процедуры
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'p_ParaLevel: '||p_ParaLevel||', p_Limit: '||p_Limit, p_msg_type => it_log.c_msg_type__debug );
    END IF;

    -- выражение для определения чанков
    x_ChunkSql := 'SELECT level, level FROM DUAL CONNECT BY LEVEL <= '||p_ParaLevel;

    -- выражение для процедуры параллельного исполнения
    x_SqlStmt := q'[
       DECLARE
         x_StartID number := :start_id ; x_EndID number := :end_id;
       BEGIN
         RSI_SUMCONFEXP.ExecParallelProc( x_StartID, ']'||p_CalcID||q'[', ]'||p_Limit||q'[, ']'||p_GUID||q'[' );
       EXCEPTION
           when others then
             it_error.put_error_in_stack;
             it_log.log( p_msg => 'Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
             it_error.clear_error_stack;
             RAISE;
       END;
    ]';

    -- запуск параллельного процесса
    it_parallel_exec.run_task_chunks_by_sql ( 
       p_parallel_level => p_ParaLevel
       , p_chunk_sql => x_ChunkSql
       , p_sql_stmt => x_SqlStmt 
    );

    -- Сообщение о завершении процедуры
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'End: '||ElapsedTime(x_StartTime), p_msg_type => it_log.c_msg_type__debug );
    END IF;
  END ExecParallel;

  /** 
   @brief    Возвращает Sql-выражение для определения ДБО для режима ЦБ на дату
  */
  FUNCTION GetDlContr0Sql ( p_DlContrID IN NUMBER, p_ClientID IN NUMBER, p_FIID IN NUMBER, p_Limit IN NUMBER DEFAULT 0) RETURN varchar2
  IS
    v_sql varchar2(32000);
  BEGIN
      v_sql:=' SELECT :p_GUID, 0, q.t_DlContrID
        FROM (SELECT '||case when p_DlContrID > 0 then '/*+ leading(mp) */' end||' DISTINCT mp.t_DlContrID
                FROM dmcaccdoc_dbt accd, ddlcontrmp_dbt mp
               WHERE accd.t_CatID = 364 ';-- t_Code = 'ЦБ Клиента, ВУ'

      if (p_ClientID > 0) then
        v_sql := v_sql ||' AND accd.t_Owner = '||p_ClientID ;
      end if;

      if (p_FIID > 0) then
        v_sql := v_sql ||' AND accd.t_Currency = '|| p_FIID;
      end if; 

      v_sql := v_sql ||'          
                 AND accd.t_IsCommon = ''X''
                 AND accd.t_ActivateDate <= :p_OnDate
                 AND (accd.t_DisablingDate = TO_DATE(''01.01.0001'',''DD.MM.YYYY'') or accd.t_DisablingDate >= :p_OnDate)
                 AND rsb_account.restac(accd.t_Account, accd.t_Currency, :p_OnDate, accd.t_Chapter, null) <> 0
                 AND mp.t_SfContrID = accd.t_ClientContrID ';

      if (p_DlContrID > 0) then
        v_sql := v_sql ||' AND mp.t_DlContrID = '|| p_DlContrID ;
      end if; 

      v_sql := v_sql ||') q' ;

      if (p_Limit > 0) then
        v_sql := v_sql ||' FETCH NEXT '||p_Limit||' ROWS ONLY ';
      end if; 

     RETURN v_sql;
  END GetDlContr0Sql;

  /** 
   @brief    Возвращает Sql-выражение для определения ДБО для режима 'Переведенные другому брокеру' с не заданной датой
  */
  FUNCTION GetDlContr1NoDateSql ( p_DlContrID IN NUMBER, p_ClientID IN NUMBER, p_FIID IN NUMBER, p_Limit IN NUMBER DEFAULT 0 ) RETURN varchar2
  IS
    v_sql varchar2(32000);
  BEGIN
    v_sql:=' SELECT DISTINCT :p_GUID, 0, t_DlContrID
          FROM (
          SELECT DISTINCT mp.t_DlContrID
          FROM (SELECT t_Kind_Operation 
                  FROM doprkoper_dbt 
                 WHERE t_DocKind = :DL_AVRWRT
                   AND Rsb_Secur.IsAvrWrtOut(rsb_secur.get_OperationGroup(t_SysTypes)) = 1) opr, 
                   ddl_tick_dbt tk, ddlcontrmp_dbt mp
          WHERE tk.t_BOfficeKind = :DL_AVRWRT
            AND tk.t_DealType = opr.t_Kind_Operation
            AND tk.t_ClientID > 0 ';

     if (p_ClientID > 0) then
        v_sql := v_sql ||' AND tk.t_ClientID = '||p_ClientID ;
     end if;

     if (p_FIID > 0) then
       v_sql := v_sql ||' AND tk.t_PFI = '|| p_FIID;
     end if; 

     v_sql := v_sql ||'          
            AND tk.t_DealDate <= :p_CurDate-1
            AND RSB_SECUR.GetMainObjAttrNoDate(:OBJTYPE_SECDEAL, LPAD(tk.t_DealID, 34, ''0''), 111 /*Внешняя операция*/) = 2 /*Да*/
            AND tk.t_DealStatus = 20 --Закрыта
            AND mp.t_SfContrID = tk.t_ClientContrID ';

     if (p_DlContrID > 0) then
        v_sql := v_sql ||' AND mp.t_DlContrID = '|| p_DlContrID ;
     end if; 

     v_sql := v_sql ||' ) q' ;

     if (p_Limit > 0) then
       v_sql := v_sql ||' FETCH NEXT '||p_Limit||' ROWS ONLY ';
     end if; 

     RETURN v_sql;

  END GetDlContr1NoDateSql;

  /** 
   @brief    Возвращает Sql-выражение для определения ДБО для режима 'Переведенные другому брокеру' с заданной датой
  */
  FUNCTION GetDlContr1DateSql ( p_DlContrID IN NUMBER, p_ClientID IN NUMBER, p_FIID IN NUMBER, p_Limit IN NUMBER DEFAULT 0 ) RETURN varchar2
  IS
    v_sql varchar2(32000);
  BEGIN
    v_sql:=' SELECT DISTINCT :p_GUID, 0, t_DlContrID
          FROM (
          SELECT DISTINCT mp.t_DlContrID
          FROM (SELECT t_Kind_Operation 
                  FROM doprkoper_dbt 
                 WHERE t_DocKind = :DL_AVRWRT
                   AND Rsb_Secur.IsAvrWrtOut(rsb_secur.get_OperationGroup(t_SysTypes)) = 1) opr, 
                   ddl_tick_dbt tk, ddlcontrmp_dbt mp
          WHERE tk.t_BOfficeKind = :DL_AVRWRT
            AND tk.t_DealType = opr.t_Kind_Operation
            AND tk.t_ClientID > 0 ';

     if (p_ClientID > 0) then
        v_sql := v_sql ||' AND tk.t_ClientID = '||p_ClientID ;
     end if;

     if (p_FIID > 0) then
       v_sql := v_sql ||' AND tk.t_PFI = '|| p_FIID;
     end if; 

     v_sql := v_sql ||'          
            AND tk.t_DealDate <= :x_OnDate AND tk.t_DealDate >= :x_OnDate - 30
            AND RSB_SECUR.GetMainObjAttrNoDate(:OBJTYPE_SECDEAL, LPAD(tk.t_DealID, 34, ''0''), 111 /*Внешняя операция*/) = 2 /*Да*/
            AND tk.t_DealStatus = 20 --Закрыта
            AND mp.t_SfContrID = tk.t_ClientContrID ';

     if (p_DlContrID > 0) then
        v_sql := v_sql ||' AND mp.t_DlContrID = '|| p_DlContrID ;
     end if; 

     v_sql := v_sql ||' ) q' ;

     if (p_Limit > 0) then
       v_sql := v_sql ||' FETCH NEXT '||p_Limit||' ROWS ONLY ';
     end if; 

     RETURN v_sql;

  END GetDlContr1DateSql;

  /** 
   @brief    Получение данных для отчета
  */
  PROCEDURE CreateSumConfirmExpRepDataInter( 
      p_GUID IN VARCHAR2
      , p_OnDate IN DATE
      , p_CurDate IN DATE
      , p_LaunchMode IN NUMBER
      , p_ClientID IN NUMBER
      , p_DlContrID IN NUMBER
      , p_FIID IN NUMBER
  )
  IS
    v_task_name VARCHAR2(30);
    v_sql_chunks CLOB;
    v_sql_process VARCHAR2(400);
    v_try NUMBER(5) := 0;
    v_status NUMBER;

    TYPE masexec_t IS TABLE OF DMASEXEC_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
    v_masexec masexec_t;
    v_MaxPackNum NUMBER(10);
    x_clob clob;
    x_OnDate DATE := p_OnDate;
    x_CalcID VARCHAR2(64);
    v_sql varchar2(32000);
    x_Cnt NUMBER := 0;
  BEGIN
    x_clob := 
        to_clob('p_GUID: ['||p_GUID||']'||chr(13)||chr(10))
        || to_clob('p_OnDate: ['||to_char(p_OnDate, 'yyyy-mm-dd')||']'||chr(13)||chr(10))
        || to_clob('p_CurDate: ['||to_char(p_CurDate, 'yyyy-mm-dd')||']'||chr(13)||chr(10))
        || to_clob('p_LaunchMode: ['||to_char(p_LaunchMode)||']'||chr(13)||chr(10))
        || to_clob('p_ClientID: ['||to_char(p_ClientID)||']'||chr(13)||chr(10))
        || to_clob('p_DlContrID: ['||to_char(p_DlContrID)||']'||chr(13)||chr(10))
        || to_clob('p_FIID: ['||to_char(p_FIID)||']'||chr(13)||chr(10))
    ;
    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'Start', p_msg_clob => x_clob ) ;
    END IF;

    DELETE FROM DSCSUMCONFEXP_DBT WHERE t_GUID = p_GUID;
    DELETE FROM DMASEXEC_DBT WHERE t_GUID = p_GUID;

    -- DEF-72823, нужно обнулить данные перед использованием
    x_ViewedSubst.DELETE;
    x_ViewedDepo.DELETE;
    DELETE FROM dmatchticks_tmp;

    IF (p_LaunchMode = 0) THEN 
      -- ЦБ на дату
      v_sql := GetDlContr0Sql( p_DlContrID, p_ClientID, p_FIID);
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Bulk collect', p_msg_clob => to_clob(v_sql) ) ;
      END IF;
      EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_masexec USING p_GUID, p_OnDate, p_OnDate, p_OnDate;
    ELSIF ((p_LaunchMode = 1) AND (x_OnDate = g_NullDate)) THEN 
      -- Переведенные другому брокеру с не заданной датой
      v_sql := GetDlContr1NoDateSql( p_DlContrID, p_ClientID, p_FIID);
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Bulk collect', p_msg_clob => to_clob(v_sql) ) ;
      END IF;
      EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_masexec USING p_GUID, p_CurDate;
    ELSIF (p_LaunchMode = 1) THEN 
      -- Переведенные другому брокеру с заданной датой
      v_sql := GetDlContr1DateSql( p_DlContrID, p_ClientID, p_FIID);
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Bulk collect', p_msg_clob => to_clob(v_sql) ) ;
      END IF;
      EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_masexec USING p_GUID, p_OnDate, p_OnDate;
    END IF;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'v_masexec.COUNT: '||v_masexec.COUNT ) ;
    END IF;

    IF v_masexec.COUNT > 0 THEN
      -- DEF-88733 Нужно заполнить DMASEXEC_DBT (и даже для одного договора), так как через него теперь работает печать через FuncObj
      FOR i IN v_masexec.FIRST .. v_masexec.LAST LOOP
        INSERT INTO DMASEXEC_DBT r (
          r.t_guid, r.t_packnum, r.t_id
        ) VALUES (
          p_GUID, 1, v_masexec(i).t_ID
        );
      END LOOP;
      IF v_masexec.COUNT = 1 THEN
        RSI_SUMCONFEXP.CreateSumConfirmExpRepDataByContr(v_masexec(1).t_ID, v_masexec(1).t_ID, p_GUID, x_OnDate, p_CurDate, p_LaunchMode, p_FIID, x_Cnt);
      ELSE
        -- запускаем параллельную обработку
        x_CalcID := it_parallel_exec.init_calc();
        -- формируем задания для параллельного сбора данных
        FOR i IN v_masexec.FIRST .. v_masexec.LAST LOOP
          x_Cnt := x_Cnt + GenChunk ( x_CalcID, v_masexec(i).t_ID, p_FIID, p_LaunchMode, x_OnDate, p_CurDate );
        END LOOP;
        IF(g_DebugFlag = 1) THEN
          it_log.log( p_msg => 'Сгенерировано заданий: '||x_Cnt ) ;
        END IF;
        -- вызов процедуры для параллельного сбора данных
        RSI_SUMCONFEXP.ExecParallel(x_CalcID, PARALLEL_LEVEL, 0, p_GUID);
        -- очистка
        v_masexec.DELETE;
        it_parallel_exec.clear_calc( x_CalcID );
      END IF;
    END IF;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'End' ) ;
    END IF;

  END CreateSumConfirmExpRepDataInter;

  /** 
   @brief    Получение данных для отчета
  */
  PROCEDURE CreateSumConfirmExpRepData( 
      p_GUID IN VARCHAR2
      , p_OnDate IN DATE
      , p_LaunchMode IN NUMBER
      , p_ClientID IN NUMBER
      , p_DlContrID IN NUMBER
      , p_FIID IN NUMBER
  )
  IS
    x_CurDate DATE := RsbSessionData.curdate;
  BEGIN
    CreateSumConfirmExpRepDataInter(p_GUID, p_OnDate, x_CurDate, p_LaunchMode, p_ClientID, p_DlContrID, p_FIID);
  END CreateSumConfirmExpRepData;

  /** 
   @brief    Установка флага отладки
  */
  PROCEDURE SetDebugFlag( p_DebugFlag IN number  )
  IS
  BEGIN
    g_DebugFlag := p_DebugFlag;
  END SetDebugFlag;

  /**
   @brief    Функция инициализации отчета для параллельной работы
   @return   номер расчета (см. itt_parallel_exec.calc_id)
  */
  FUNCTION InitParallelCalc RETURN varchar2 IS
  BEGIN
    RETURN to_char( it_parallel_exec.init_calc() );
  EXCEPTION
    WHEN others THEN
     RETURN ''; 
  END InitParallelCalc;

  /**
   @brief    Процедура очистки данных отчета
   @param[in]	p_RepID   	номер расчета (см. itt_parallel_exec.calc_id)
  */
  PROCEDURE ClearParallelCalc(p_RepID IN varchar2)
  IS
  BEGIN
    it_parallel_exec.clear_calc(p_RepID);
  END ClearParallelCalc;

  /**
   @brief    Процедура удаления данных отчета
   @param[in]	p_RepID   	номер расчета (см. itt_parallel_exec.calc_id)
  */
  PROCEDURE DeleteParallelCalc(p_RepID IN varchar2)
  IS
  BEGIN
    EXECUTE IMMEDIATE 'DELETE FROM itt_parallel_exec partition (p'||p_RepID||')';
    COMMIT;
  END DeleteParallelCalc;


  /**
   @brief    Запуск процесса получения данных для отчета. Запускается сервисом ExecuteCode через QManager
   @param[in]  p_worklogid     	ID задания (itt_q_message_log.msgid)
   @param[in]  p_messmeta     	мета-данные задания
  */
  PROCEDURE CallProcess ( p_worklogid integer, p_messmeta  xmltype )
  IS
    x_CalcID varchar2(25);
    x_ProcessNo NUMBER;
    x_Limit NUMBER;
    x_GUID varchar2(32);
    x_DebugFlag NUMBER;
  BEGIN
    -- считывание параметров
    WITH meta AS 
      (select p_messmeta as x from dual)
      SELECT 
        EXTRACTVALUE(meta.x, '/XML/@CalcID')
        , to_number(EXTRACTVALUE(meta.x, '/XML/@ProcessNo'))
        , to_number(EXTRACTVALUE(meta.x, '/XML/@Limit'))
        , EXTRACTVALUE(meta.x, '/XML/@GUID')
        , to_number(EXTRACTVALUE(meta.x, '/XML/@DebugFlag'))
      INTO x_CalcID, x_ProcessNo, x_Limit, x_GUID, x_DebugFlag
      FROM meta
    ;
    -- запуск процесса
    IF(x_CalcID is not null) THEN
      SetDebugFlag( x_DebugFlag );
      ExecParallelProc ( x_ProcessNo, x_CalcID, x_Limit, x_GUID );
    END IF;
  END CallProcess;


  /**
   @brief    Заполнение таблицы DMASEXEC_DBT по заданиям из itt_parallel_exec
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_Guid   	GUID отчета.
  */
  FUNCTION SetMasExec( p_CalcID IN varchar2, p_GUID IN varchar2 ) RETURN NUMBER
  IS
    x_ChunkCnt number := 0;
    x_Cnt number;
    x_PackNum number := 1;
  BEGIN
    delete from dmasexec_dbt where t_guid = p_GUID;
    insert into dmasexec_dbt (t_guid, t_packnum, t_id)
      select distinct p_GUID AS t_guid, 0 AS t_packnum, r.num01 AS t_id
        from itt_parallel_exec r where r.calc_id = p_CalcID
    ;
    x_ChunkCnt := SQL%ROWCOUNT;
    IF(x_Cnt < 100) THEN
      update dmasexec_dbt set t_packnum = 1 where t_guid = p_GUID;
    ELSE
      -- заданий много, нужна корректировка пачек
      x_Cnt := 0;
      for i in (
         select distinct pt.t_Name, r.num01 AS t_dlcontrid
           from itt_parallel_exec r 
           inner join ddlcontr_dbt dl on (dl.t_DlContrID = r.num01)
           inner join dsfcontr_dbt sf on (sf.t_ID = dl.t_SfContrID)
           inner join dparty_dbt pt on (pt.t_PartyID = sf.t_PartyID)
           where r.calc_id = p_CalcID
          order by pt.t_Name, r.num01
       ) loop
         if(x_Cnt < 100) then
           x_Cnt := x_Cnt + 1;
         else 
           x_PackNum := x_PackNum + 1;
           x_Cnt := 1;
         end if;
         update dmasexec_dbt m 
           set m.t_packnum = x_PackNum
           where m.t_guid = p_Guid and m.t_id = i.t_dlcontrid
         ;
      end loop;
    END IF;
    commit;
    RETURN x_ChunkCnt;
  END SetMasExec;

  /**
   @brief    Запуск сервиса ExecuteCode через QManager
   @param[in]  p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[in]  p_ParallelCnt   	Количество параллельных процессов
   @param[in]  p_Limit   	Ограничитель выполняемых заданий. Если 0, выполняются все.
   @param[in]  p_Guid   	GUID отчета.
  */
  PROCEDURE CallExecuteCodeInQManager( p_CalcID IN varchar2, p_ParallelCnt IN NUMBER, p_Limit IN NUMBER, p_GUID IN varchar2 )
  IS
    x_MessMETA xmltype;
    x_msgID itt_q_message_log.msgid%type;
    x_ParallelCnt number := p_ParallelCnt;
  BEGIN
    IF(SetMasExec( p_CalcID, p_GUID ) = 0) THEN
      -- если заданий нет, процесс не запускаем
      RETURN ; 
    END IF; 
    IF(x_ParallelCnt > g_ParallelLimit) THEN
      x_ParallelCnt := g_ParallelLimit;
    END IF;
    FOR i IN 1..x_ParallelCnt LOOP
      x_msgID := null;
      SELECT xmlelement("XML", xmlattributes(
         p_CalcID as "CalcID"
         , i AS "ProcessNo"
         , p_Limit as "Limit"
         , p_GUID as "GUID"
         , g_DebugFlag as "DebugFlag"
      )) 
      INTO x_MessMETA FROM dual;
      it_q_message.load_msg(
         io_msgid        => x_msgID
         , p_message_type  => it_q_message.C_C_MSG_TYPE_R
         , p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
         , p_ServiceName   => 'ExecuteCode'
         , p_MESSBODY      => 'call RSI_SUMCONFEXP.CallProcess(:1, :2)'
         , p_MessMETA      => x_MessMETA
      );  
    END LOOP;
  END CallExecuteCodeInQManager;

  /**
   @brief    Генерация чанков (заданий) для itt_parallel_exec по всем клиентам с полученным ФИ
   @param[in]  p_CalcID     	номер расчета (itt_parallel_exec.calc_id)
   @param[in]  p_FIID     	ID финансового инструмента
   @param[in]  p_LaunchMode    	режим запуска отчета
   @param[in]  p_OnDate    	дата отчета
   @param[in]  p_CurDate    	дата опер.дня
   @return			Количество сгенеренных заданий
  */
  FUNCTION GenFiidChunks ( p_CalcID IN NUMBER, p_FIID IN NUMBER, p_LaunchMode IN NUMBER, p_OnDate IN DATE, p_CurDate IN DATE ) RETURN NUMBER
  IS
    x_SqlIns VARCHAR2(1000);
    x_Cnt NUMBER := 0;
  begin
    for j in (select distinct t.t_clientid from ddl_tick_dbt t where t.t_pfi = p_FIID and t.t_clientID > 0) loop
      for i in (
        select distinct t_dlcontrid
          from dsfcontr_dbt sf
          inner join ddlcontrmp_dbt mp on (mp.t_sfcontrid = sf.t_id)
          where sf.t_partyid = j.t_clientid
          and (mp.t_mpclosedate = to_date('1-1-0001', 'dd-mm-yyyy')
            or mp.t_mpclosedate < p_OnDate )
      ) loop
         x_Cnt := x_Cnt + GenChunk ( p_CalcID, i.t_dlcontrid, p_FIID, p_LaunchMode, p_OnDate, p_CurDate );
      end loop; -- i
    end loop; -- j
    RETURN x_Cnt;
  END GenFiidChunks;

  /**
   @brief    Генерация заданий для отчета (с последующей обработкой через QManager)
   @param[in]  p_Guid   	GUID отчета.
   @param[in]  p_RepDate    	дата отчета
   @param[in]  p_CurDate    	дата опер.дня
   @param[in]  p_LaunchMode    	режим запуска отчета
   @param[in]  p_ClientID     	ID клиента
   @param[in]  p_DlContrID    	ID договора брокерского обслуживания
   @param[in]  p_FIID     	ID финансового инструмента
   @param[in]  p_Limit     	Ограничитель заданий
   @param[out] p_CalcID     	ID расчета (itt_parallel_exec.calc_id)
   @param[out] p_Cnt     	Количество сгенеренных заданий
  */
  PROCEDURE GenSumConfirmChunks ( 
    p_GUID IN VARCHAR2, p_RepDate IN DATE, p_CurDate IN DATE, p_LaunchMode IN NUMBER
    , p_ClientID IN NUMBER, p_DlContrID IN NUMBER, p_FIID IN NUMBER, p_Limit IN NUMBER
    , p_CalcID OUT NUMBER, p_Cnt OUT NUMBER
  )
  IS
    TYPE masexec_t IS TABLE OF DMASEXEC_DBT%ROWTYPE INDEX BY BINARY_INTEGER;
    v_masexec masexec_t;
    x_CalcID varchar2(25);
    x_OnDate DATE := NVL(p_RepDate, g_NullDate);
    v_sql varchar2(32000);
    x_Delim varchar2(24) := chr(13)||chr(10);
    x_clob clob;
  BEGIN
    x_CalcID := InitParallelCalc();
    p_CalcID := x_CalcID;

    x_clob := 
        to_clob('p_GUID: ['||p_GUID||']'||x_Delim)
        || to_clob('x_OnDate: ['||to_char(x_OnDate, 'yyyy-mm-dd')||']'||x_Delim)
        || to_clob('p_CurDate: ['||to_char(p_CurDate, 'yyyy-mm-dd')||']'||x_Delim)
        || to_clob('p_LaunchMode: ['||to_char(p_LaunchMode)||']'||x_Delim)
        || to_clob('p_ClientID: ['||to_char(p_ClientID)||']'||x_Delim)
        || to_clob('p_DlContrID: ['||to_char(p_DlContrID)||']'||x_Delim)
        || to_clob('p_FIID: ['||to_char(p_FIID)||']'||x_Delim)
        || to_clob('x_CalcID: ['||to_char(x_CalcID)||']'||x_Delim)
    ;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'Start', p_msg_clob => x_clob ) ;
    END IF;

    IF (p_LaunchMode = 0) THEN 
      -- ЦБ на дату
      v_sql := GetDlContr0Sql( p_DlContrID, p_ClientID, p_FIID, p_Limit);
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Bulk collect 0', p_msg_clob => to_clob(v_sql) ) ;
      END IF;
      EXECUTE IMMEDIATE v_sql 
        BULK COLLECT INTO v_masexec 
        USING p_GUID, x_OnDate, x_OnDate, x_OnDate
      ;
    ELSIF ((p_LaunchMode = 1) AND (x_OnDate = g_NullDate)) THEN 
      -- Переведенные другому брокеру с не заданной датой
      v_sql := GetDlContr1NoDateSql( p_DlContrID, p_ClientID, p_FIID, p_Limit );
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Bulk collect 1 no date', p_msg_clob => to_clob(v_sql) ) ;
      END IF;
      EXECUTE IMMEDIATE v_sql 
        BULK COLLECT INTO v_masexec 
        USING p_GUID, RSB_SECUR.DL_AVRWRT, RSB_SECUR.DL_AVRWRT, p_CurDate, RSB_SECUR.OBJTYPE_SECDEAL
      ;
    ELSIF (p_LaunchMode = 1) THEN 
      -- Переведенные другому брокеру с заданной датой
      v_sql := GetDlContr1DateSql( p_DlContrID, p_ClientID, p_FIID, p_Limit );
      IF(g_DebugFlag = 1) THEN
        it_log.log( p_msg => 'Bulk collect 1 with date', p_msg_clob => to_clob(v_sql) ) ;
      END IF;
      EXECUTE IMMEDIATE v_sql 
        BULK COLLECT INTO v_masexec 
        USING p_GUID, RSB_SECUR.DL_AVRWRT, RSB_SECUR.DL_AVRWRT, x_OnDate, x_OnDate, RSB_SECUR.OBJTYPE_SECDEAL
      ;
    END IF;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'v_masexec.COUNT: '||v_masexec.COUNT ) ;
    END IF;

    p_Cnt := 0;
    IF v_masexec.COUNT > 0 THEN
      FOR i IN v_masexec.FIRST .. v_masexec.LAST LOOP
        p_Cnt := p_Cnt + GenChunk ( x_CalcID, v_masexec(i).t_ID, p_FIID, p_LaunchMode, x_OnDate, p_CurDate );
      END LOOP;
    END IF;

    -- очистка
    v_masexec.DELETE;

    IF(g_DebugFlag = 1) THEN
      it_log.log( p_msg => 'x_CalcID: '||x_CalcID||', сгенерировано заданий: '||p_Cnt ) ;
    END IF;

  EXCEPTION
    WHEN others THEN
      it_error.put_error_in_stack;
      it_log.log( p_msg => 'Err: '||SQLERRM, p_msg_type => it_log.c_msg_type__error );
      it_error.clear_error_stack;
      RAISE;

  END GenSumConfirmChunks;

END RSI_SUMCONFEXP;
