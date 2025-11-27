CREATE OR REPLACE PACKAGE RSI_RsbAccOvervalue IS
/**
 * Пакет функций и процедур для работы процедуры урегилирования покрытий
 */

    SET_CHAR CONSTANT CHAR(1) := 'X';
  UNSET_CHAR CONSTANT CHAR(1) := chr(0);

  NATCUR CONSTANT INTEGER := 0; -- ИД национальной валюты

  --
  -- режимы учета парности счетов курсовых разниц
  --
  PAIR_MODE_NONE       CONSTANT INTEGER := 0; -- не учитывать парность счетов курсовых разниц
  PAIR_MODE_PAIR       CONSTANT INTEGER := 1; -- учитывать парность счетов курсовых разниц
  PAIR_MODE_CHECK_REST CONSTANT INTEGER := 2; -- учитывать парность счетов курсовых разниц c контролем остатков

  TA_PAIR CONSTANT CHAR(1) := 'Ш';

  --
  -- вспомогательные типы
  --
  TYPE Chapter_t IS TABLE OF daccovervalue_tmp.t_Chapter%TYPE;

  TYPE ExRateAccountPlus_t  IS TABLE OF daccovervalue_tmp.t_ExRateAccountPlus%TYPE;
  TYPE ExRateAccountMinus_t IS TABLE OF daccovervalue_tmp.t_ExRateAccountMinus%TYPE;

  TYPE tt_daccovervalue is table of daccovervalue_tmp%rowtype ;
  TYPE tc_daccovervalue IS REF CURSOR RETURN daccovervalue_tmp%ROWTYPE ;
  
  --
  -- Определить, являются ли счета парными
  --
  FUNCTION definePairExRateAccounts( p_ExRateAccountPlus    IN  STRING
                                    ,p_ExRateAccountMinus   IN  STRING
                                    ,p_Chapter              IN  INTEGER
                                    ) RETURN CHAR deterministic ;
  
  --
  -- Определить счета курсовых разниц
  --
  PROCEDURE GetExRateAccounts( p_Version IN INTEGER );

  --
  -- Первоначальная инициализация процедуры переоценки
  --
  PROCEDURE InitProcedure( p_Version IN INTEGER );

  --
  -- Определение сумм переоценки по урегулируемым счетам покрытия
  --
  FUNCTION CalcExRateSum( p_RegDate IN DATE, p_RestDate IN DATE, p_ZeroRest IN CHAR ) RETURN INTEGER;

  --
  -- Переопределение сумм переоценки по урегулируемым счетам покрытия
  --
  FUNCTION ReCalcExRateSum( p_RegDate IN DATE, p_RestDate IN DATE, p_ZeroRest IN CHAR ) RETURN INTEGER;

  FUNCTION procCalcExRateSumAcc( pc_daccovervalue tc_daccovervalue
                                ,p_RegDate IN DATE
                                ,p_RestDate IN DATE
                                ,p_ZeroRest IN CHAR ) return tt_daccovervalue parallel_enable(partition pc_daccovervalue by any) pipelined ;

  --
  -- Определение счетов курсовых разниц, которые будут участвовать в проводках переоценки
  --
  FUNCTION DefineExRateAccount( p_RegDate IN DATE, p_PairMode IN INTEGER, p_Version IN INTEGER ) RETURN INTEGER;

  --
  -- Определить, какой счет курсовой разницы использовать для переоценки
  --
  FUNCTION CorrectExRateAccount( p_ExRateAccountPlus  IN OUT STRING
                                ,p_ExRateAccountMinus IN OUT STRING
                                ,p_Chapter            IN INTEGER
                                ,p_RegDate            IN DATE
                                ,p_PairMode           IN INTEGER
                                ,p_ErrorMessage       OUT daccovervalue_tmp.t_ErrorMessage%TYPE
                               )
  RETURN INTEGER;

  --
  -- Получить начальное значение номера проводки переоценки
  --
  FUNCTION GetStartNumberDoc( p_ParamID INTEGER )
  RETURN INTEGER;
  
END;
/
