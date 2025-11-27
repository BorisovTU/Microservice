-- Author  : Vrubel Vyacheslav
-- Purpose : Пакет подготовки данных для отчета "Реестр ВУ"

create or replace package rsb_dlregisteria
is
  ACC_TYPE_SEC CONSTANT INTEGER := 1; --ЦБ
  ACC_TYPE_FIN CONSTANT INTEGER := 2; --ДС
  ACC_TYPE_FO  CONSTANT INTEGER := 3; --ФО

  V_SESSIONID NUMBER(20) := 0;
  V_CALCID NUMBER(20) := 0;
  V_LINKID NUMBER(20) := 0;
  
  CATID_SEC CONSTANT INTEGER := 368;
  CATID_FIN CONSTANT INTEGER := 349;
  CATID_FO CONSTANT INTEGER := 365;

  CHUNK_COUNT CONSTANT INTEGER := 32;

  ACTIONID_PARALLELSTARTED CONSTANT INTEGER := 100;
  ACTIONID_CHUNKSTARTED CONSTANT INTEGER := 101;
  ACTIONID_CHUNKFINISHED CONSTANT INTEGER := 102;
  ACTIONID_PARALLELFINISHED CONSTANT INTEGER := 104;

  PROCEDURE CreateData_Contr(BeginDate      IN DATE,
                             EndDate        IN DATE,
                             StockMarket    IN CHAR,
                             FuturesMarket  IN CHAR,
                             CurrencyMarket IN CHAR,
                             Sector_Brokers IN CHAR,
                             Sector_Dilers  IN CHAR,
                             Sector_Clients IN CHAR,
                             Block_Deals    IN CHAR,
                             Block_Clients  IN CHAR,
                             Block_InAcc    IN CHAR,
                             SelectedClients IN NUMBER,
                             SelectedContrs   IN NUMBER);

  PROCEDURE CreateData_DV_NDeals_parallel(Calc_id number,
                                 Row_ID_start number,
                                 Row_ID_end   number,
                                 BeginDate      IN DATE,
                                 EndDate        IN DATE,
                                 StockMarket    IN CHAR,
                                 FuturesMarket  IN CHAR,
                                 CurrencyMarket IN CHAR,
                                 Sector_Brokers IN CHAR,
                                 Sector_Dilers  IN CHAR,
                                 Sector_Clients IN CHAR,
                                 Block_Deals    IN CHAR,
                                 Block_Clients  IN CHAR,
                                 Block_InAcc    IN CHAR,
                                 SelectedClients IN NUMBER,
                                 SelectedContrs   IN NUMBER) ;
                                 
  PROCEDURE CreateData_SC_Deals_parallel(Calc_id  in number,
                                Row_ID_start in number,
                                Row_ID_end  in number,
                                BeginDate      IN DATE,
                                EndDate        IN DATE,
                                StockMarket    IN CHAR,
                                FuturesMarket  IN CHAR,
                                CurrencyMarket IN CHAR,
                                Sector_Brokers IN CHAR,
                                Sector_Dilers  IN CHAR,
                                Sector_Clients IN CHAR,
                                Block_Deals    IN CHAR,
                                Block_Clients  IN CHAR,
                                Block_InAcc    IN CHAR,
                                SelectedClients IN NUMBER,
                                SelectedContrs   IN NUMBER);
                                
  PROCEDURE CreateData_Deals(BeginDate      IN DATE,
                              EndDate        IN DATE,
                              StockMarket    IN CHAR,
                              FuturesMarket  IN CHAR,
                              CurrencyMarket IN CHAR,
                              Sector_Brokers IN CHAR,
                              Sector_Dilers  IN CHAR,
                              Sector_Clients IN CHAR,
                              Block_Deals    IN CHAR,
                              Block_Clients  IN CHAR,
                              Block_InAcc    IN CHAR,
                              SelectedClients IN NUMBER,
                              SelectedContrs   IN NUMBER);
  
  FUNCTION MakeCSV(p_report_date IN DATE, p_report_part IN INTEGER, p_count_part IN INTEGER, p_part IN INTEGER) return number;
  
  PROCEDURE MakeAllCSV(p_report_date IN DATE, p_count_part IN INTEGER);

  PROCEDURE GenCalcIds(SessionID OUT NUMBER, CalcID OUT NUMBER);

  PROCEDURE SetCalcIds(SessionID IN NUMBER, CalcID IN NUMBER, LinkID IN NUMBER DEFAULT NULL);

  PROCEDURE PushLogLine (LogMessage           IN VARCHAR2,
                         ProgressMessage      IN VARCHAR2 DEFAULT NULL,
                         ActionID             IN NUMBER DEFAULT NULL,
                         LinkID               IN NUMBER DEFAULT NULL
                       );

  FUNCTION GetLastProgressMessage return VARCHAR2;

  function GetLastParallProcId(SessionID IN NUMBER, CalcID IN NUMBER) return NUMBER;

  FUNCTION GenLinkId return NUMBER;

  function GetParallProcProgress(SessionID IN NUMBER, CalcID IN NUMBER, LinkID IN NUMBER) return NUMBER;

  PROCEDURE DropBufData(p_start integer default 0);

  PROCEDURE SetIdsDropBufData(p_start integer , SessionID IN NUMBER, CalcID IN NUMBER);

  PROCEDURE AddAccounts_Contr_Parall(start_id in NUMBER, end_id NUMBER, BeginDate IN DATE, EndDate IN DATE);

end rsb_dlregisteria;
/
