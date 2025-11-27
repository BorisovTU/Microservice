CREATE OR REPLACE PACKAGE BODY RSI_NPTX
IS

    FUNCTION iif( Cond IN BOOLEAN, n1 IN NUMBER, n2 IN NUMBER )
      RETURN NUMBER
    IS
    BEGIN
      IF( Cond ) THEN
         RETURN n1;
      ELSE
         RETURN n2;
      END IF;
    END;

    FUNCTION iif( Cond IN BOOLEAN, n1 IN DATE, n2 IN DATE )
      RETURN DATE
    IS
    BEGIN
      IF( Cond ) THEN
         RETURN n1;
      ELSE
         RETURN n2;
      END IF;
    END;

    FUNCTION iif( Cond IN BOOLEAN, n1 IN VARCHAR2, n2 IN VARCHAR2 )
      RETURN VARCHAR2
    IS
    BEGIN
      IF( Cond ) THEN
         RETURN n1;
      ELSE
         RETURN n2;
      END IF;
    END;

  ----Задает приоритет приходов зависимости от типов сделок прихода для сделок продаж.
    FUNCTION GetBuyOrderForSale( p_BuyKind IN NUMBER )
       RETURN NUMBER DETERMINISTIC
    IS
    BEGIN
       IF( p_BuyKind = RSI_NPTXC.NPTXLOTS_BUY ) THEN
          RETURN 1;
       ELSE --Репо, Займ
          RETURN 2;
       END IF;
    END; --GetBuyOrderForSale

  ----Задает приоритет приходов зависимости от типов сделок прихода для сделок Репо прямых.
    FUNCTION GetBuyOrderForRepo( p_BuyKind IN NUMBER )
       RETURN NUMBER DETERMINISTIC
    IS
    BEGIN
       IF( p_BuyKind = RSI_NPTXC.NPTXLOTS_BUY ) THEN
          RETURN 2;
       ELSE --Репо, Займ
          RETURN 1;
       END IF;
    END; --GetBuyOrderForRepo

  ----Определяет по виду сделки, является ли она продажей (выбытием). Для сделки из двух частей - по первой части
    FUNCTION IsSale( p_Kind IN NUMBER )
       RETURN NUMBER DETERMINISTIC
    IS
    BEGIN
       IF( p_Kind IN (RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT )) THEN
          RETURN 1;
       ELSE
          RETURN 0;
       END IF;
    END; --IsSale

  ----Определяет по типу сделки, является ли она виртуальной.
    FUNCTION IsVirtual( p_Type IN NUMBER )
       RETURN NUMBER DETERMINISTIC
    IS
    BEGIN
       IF( p_Type IN (RSI_NPTXC.NPTXDEAL_MARKET, RSI_NPTXC.NPTXDEAL_CALC )) THEN
          RETURN 1;
       ELSE
          RETURN 0;
       END IF;
    END; --IsVirtual

  ----Вычисляет значение признака наличия свободного остатка в зависимости от параметров лота.
    FUNCTION GetIsFree( p_AMOUNT IN NUMBER, p_SALE IN NUMBER, p_RETFLAG IN CHAR, p_INACC IN CHAR,
                        p_BLOCKED IN CHAR, p_BuyDate IN DATE, p_SaleDate IN DATE )
      RETURN CHAR DETERMINISTIC
    IS
    BEGIN
      IF ((p_AMOUNT - p_SALE > 0) AND
          (p_RETFLAG = CHR(0)) AND
          (p_INACC = CHR(88)) AND
          (p_BLOCKED = CHR(0) OR ReestrValue.W1 = RSI_NPTXC.NPTXREG_W1_YES) AND
          (p_BuyDate <> p_SaleDate)
         ) THEN
         RETURN CHR(88);
      ELSE
         RETURN CHR(0);
      END IF;
    END;--TXGetIsFree

  ----Формирует код виртуальной сделки
    FUNCTION GenVirtNum( pKind IN NUMBER, pType IN NUMBER, pDate IN DATE, pCount IN NUMBER )
      RETURN VARCHAR2
    IS
    BEGIN

      RETURN 'V'||
             iif(pKind = RSI_NPTXC.NPTXLOTS_SALE, 'S', 'B')||
             iif(pType = RSI_NPTXC.NPTXDEAL_MARKET, 'M', 'C')||
             TO_CHAR(pDate, 'DDMMYY')||
             '/'||
             LTRIM(TO_CHAR(pCount,'FM09999'));
    END;

  ----Получить кол-во виртуальных лотов по его номеру
    FUNCTION GetVirtCountByNum( in_Number IN VARCHAR2 )
      RETURN NUMBER
    IS
      v_index NUMBER;
      v_num   NUMBER;
    BEGIN
      SELECT INSTR(in_Number, '/') + 1
        INTO v_index
        FROM dual;

      SELECT TO_NUMBER(SUBSTR(in_Number, v_index))
        INTO v_num
        FROM dual;

      RETURN v_num;
    END;

  ---- Возвращает ID ВН
    FUNCTION GetFaceValueFI( pFIID IN NUMBER )
      RETURN NUMBER
    IS
       out_FaceValueFIID NUMBER;
    BEGIN
       RETURN v_FaceValueFIID(pFIID);

    EXCEPTION
      WHEN NO_DATA_FOUND THEN 
        BEGIN
          SELECT fin.t_FaceValueFI
            INTO out_FaceValueFIID
            FROM dfininstr_dbt fin
           WHERE fin.t_FIID = pFIID;

           v_FaceValueFIID(pFIID) := out_FaceValueFIID;
           RETURN out_FaceValueFIID;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN 
            BEGIN
              v_FaceValueFIID(pFIID) := -1;
            END;
        END;
    END;

  ----Расчитывает курс НКД (за 1 шт.) по ценной бумаге на дату.
    FUNCTION GetNKDCource( pFIID IN NUMBER, pDate IN DATE )
    RETURN NUMBER
    IS
      v_NKD NUMBER;
    BEGIN
      RETURN v_NKDCourceValue(pDate - c_BegCourceDate)(pFIID);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN 
         BEGIN

           v_NKD := RSI_RSB_FIInstr.FI_CalcNKD(pFIID, pDate, 1, 0);
           v_NKDCourceValue(pDate - c_BegCourceDate)(pFIID) := v_NKD;
         END;

      RETURN v_NKD;
    END; --GetNKDCource

    --Проверить наличие категории
    FUNCTION CheckCateg( ObjType IN NUMBER, GroupID IN NUMBER, ObjID IN VARCHAR2, AttrID IN NUMBER )
      RETURN NUMBER DETERMINISTIC
    IS
      v_CT NUMBER;
    BEGIN

      SELECT COUNT(1)
        INTO v_CT
        FROM dobjatcor_dbt
       WHERE t_ObjectType = ObjType
         AND t_GroupID    = GroupID
         AND t_Object     = ObjID
         AND t_AttrID     = AttrID;

      IF v_CT > 0 THEN
        RETURN 1;
      END IF;

      RETURN 0;

      EXCEPTION
        WHEN NO_DATA_FOUND
          THEN RETURN 0;
    END;

    --Получить наиболее актуальное значение категории
    FUNCTION GetFirstObjAttrNoDate(p_ObjectType IN dobjatcor_dbt.t_ObjectType%TYPE,
                                  p_Object     IN dobjatcor_dbt.t_Object%TYPE,
                                  p_GroupID    IN dobjatcor_dbt.t_GroupID%TYPE
                                  )
    RETURN dobjattr_dbt.t_AttrID%TYPE
    IS
    p_AttrID dobjattr_dbt.t_AttrID%TYPE;
    BEGIN
      BEGIN
        SELECT t_AttrID INTO p_AttrID FROM (
            SELECT AtCor.t_AttrID 
              FROM dobjatcor_dbt AtCor
             WHERE AtCor.t_ObjectType  = p_ObjectType
               AND AtCor.t_GroupID     = p_GroupID
               AND AtCor.t_Object      = p_Object
              ORDER BY t_ValidToDate DESC )
          WHERE ROWNUM = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
          p_AttrID := 0;
        WHEN OTHERS
        THEN
          p_AttrID := 0;
      END;

      RETURN p_AttrID;
    END;

    -- получить тип лота в зависимости от операции
    FUNCTION get_lotKind( oGrp IN NUMBER, DealID IN NUMBER, DealPart IN NUMBER DEFAULT 1, IsPartyClient IN NUMBER DEFAULT 0 )
      RETURN NUMBER DETERMINISTIC
    IS
    BEGIN
      IF (Rsb_Secur.IsBuy(oGrp)=1 OR
          Rsb_Secur.IsAvrWrtIn(oGrp)=1) AND
         Rsb_Secur.IsRepo(oGrp)<>1 AND
         Rsb_Secur.IsBackSale(oGrp)<>1 AND
         Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN RSI_NPTXC.NPTXLOTS_BUY;
        ELSE
          RETURN RSI_NPTXC.NPTXLOTS_SALE;
        END IF;
      ELSIF (Rsb_Secur.IsSale(oGrp)=1 OR
             Rsb_Secur.IsAvrWrtOut(oGrp)=1 OR
             Rsb_Secur.IsRet_Issue(oGrp)=1) AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsBackSale(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN RSI_NPTXC.NPTXLOTS_SALE;
        ELSE
          RETURN RSI_NPTXC.NPTXLOTS_BUY;
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN RSI_NPTXC.NPTXLOTS_BACKREPO;
          else
             RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXLOTS_SALE );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN RSI_NPTXC.NPTXLOTS_REPO;
          else
             RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_BUY );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsLoan(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN RSI_NPTXC.NPTXLOTS_LOANGET;
          else
             RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXLOTS_SALE );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN RSI_NPTXC.NPTXLOTS_LOANPUT;
          else
             RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_BUY );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN RSI_NPTXC.NPTXLOTS_REPO;
          else
             RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_BUY );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN RSI_NPTXC.NPTXLOTS_BACKREPO;
          else
             RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXLOTS_SALE );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsLoan(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN RSI_NPTXC.NPTXLOTS_LOANPUT;
          else
             RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_BUY );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN RSI_NPTXC.NPTXLOTS_LOANGET;
          else
             RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXLOTS_SALE );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsBackSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXLOTS_SALE );
        ELSE
          RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_BUY );
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsBackSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_BUY );
        ELSE
          RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXLOTS_SALE );
        END IF;
      ELSIF Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_SHARE)=1 OR
            Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_RECEIPT)=1 THEN
        RETURN iif( DealPart = 1, RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_BUY );
      END IF;

      RETURN RSI_NPTXC.NPTXLOTS_UNDEF;
    END; --get_lotKind

    -- получить дату покупки лота в зависимости от операции
    FUNCTION get_lotBuyDate( oGrp IN NUMBER, DealID IN NUMBER, FactDate IN DATE, DealPart IN NUMBER DEFAULT 1, IsPartyClient IN NUMBER DEFAULT 0 )
      RETURN DATE DETERMINISTIC
    IS
    BEGIN
      IF (Rsb_Secur.IsBuy(oGrp)=1 OR
          Rsb_Secur.IsAvrWrtIn(oGrp)=1) AND
         Rsb_Secur.IsRepo(oGrp)<>1 AND
         Rsb_Secur.IsBackSale(oGrp)<>1 AND
         Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN FactDate;
        ELSE
          RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
        END IF;
      ELSIF (Rsb_Secur.IsSale(oGrp)=1 OR
             Rsb_Secur.IsAvrWrtOut(oGrp)=1 OR
             Rsb_Secur.IsRet_Issue(oGrp)=1) AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsBackSale(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
        ELSE
          RETURN FactDate;
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN FactDate;
          else
             RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
          else
             RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsLoan(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN FactDate;
          else
             RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
          else
             RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
          else
             RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN FactDate;
          else
             RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsLoan(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
          else
             RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN FactDate;
          else
             RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsBackSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
        ELSE
          RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsBackSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
        ELSE
          RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
        END IF;
      ELSIF Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_SHARE)=1 OR
            Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_RECEIPT)=1 THEN
        RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
      END IF;

      RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
    END; --get_lotBuyDate

    -- получить дату продажи лота в зависимости от операции
    FUNCTION get_lotSaleDate( oGrp IN NUMBER, DealID IN NUMBER, FactDate IN DATE, DealPart IN NUMBER DEFAULT 1, IsPartyClient IN NUMBER DEFAULT 0 )
      RETURN DATE DETERMINISTIC
    IS
    BEGIN
      IF (Rsb_Secur.IsBuy(oGrp)=1 OR
          Rsb_Secur.IsAvrWrtIn(oGrp)=1) AND
         Rsb_Secur.IsRepo(oGrp)<>1 AND
         Rsb_Secur.IsBackSale(oGrp)<>1 AND
         Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
        ELSE
          RETURN FactDate;
        END IF;
      ELSIF (Rsb_Secur.IsSale(oGrp)=1 OR
             Rsb_Secur.IsAvrWrtOut(oGrp)=1 OR
             Rsb_Secur.IsRet_Issue(oGrp)=1) AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsBackSale(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN FactDate;
        ELSE
          RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
          else
             RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN FactDate;
          else
             RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsLoan(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
          else
             RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN FactDate;
          else
             RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN FactDate;
          else
             RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
          else
             RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsLoan(oGrp)=1 THEN
        IF IsPartyClient = 0 THEN
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN FactDate;
          else
             RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
          end if;
        ELSE
          if CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(DealID, 34, '0'), 2)=0 then
             RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
          else
             RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
          end if;
        END IF;
      ELSIF Rsb_Secur.IsBuy(oGrp)=1 AND
            Rsb_Secur.IsBackSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
        ELSE
          RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
        END IF;
      ELSIF Rsb_Secur.IsSale(oGrp)=1 AND
            Rsb_Secur.IsBackSale(oGrp)=1 AND
            Rsb_Secur.IsRepo(oGrp)<>1 AND
            Rsb_Secur.IsLoan(oGrp)<>1 THEN
        IF IsPartyClient = 0 THEN
          RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
        ELSE
          RETURN iif( DealPart = 1, TO_DATE('01.01.0001','DD.MM.YYYY'), FactDate );
        END IF;
      ELSIF Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_SHARE)=1 OR
            Rsb_Secur.check_GroupStr(oGrp,rsb_secur.IS_CONV_RECEIPT)=1 THEN
        RETURN iif( DealPart = 1, FactDate, TO_DATE('01.01.0001','DD.MM.YYYY') );
      END IF;

      RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
    END; --get_lotSaleDate

    ----- получить код лота
    FUNCTION get_lotCode( p_Code IN VARCHAR2, p_Num IN NUMBER ) RETURN VARCHAR2
    IS
       v_Code VARCHAR2(36);
    BEGIN

       v_Code := p_Code || '_КП' || LPAD( p_Num, 2, '0' );

       RETURN v_Code;
    END;--get_lotCode

    -- получить название лота в зависимости от вида
    FUNCTION get_lotName( in_Kind IN NUMBER )
      RETURN VARCHAR2 DETERMINISTIC
    IS
      v_Name VARCHAR2(30);
    BEGIN
      IF in_Kind = RSI_NPTXC.NPTXLOTS_BUY THEN
        v_Name := 'Покупка';
      ELSIF in_Kind = RSI_NPTXC.NPTXLOTS_SALE THEN
        v_Name := 'Продажа';
      ELSIF in_Kind = RSI_NPTXC.NPTXLOTS_REPO THEN
        v_Name := 'Репо прямое';
      ELSIF in_Kind = RSI_NPTXC.NPTXLOTS_BACKREPO THEN
        v_Name := 'Репо обратное';
      ELSIF in_Kind = RSI_NPTXC.NPTXLOTS_LOANPUT THEN
        v_Name := 'Займ размещение';
      ELSIF in_Kind = RSI_NPTXC.NPTXLOTS_LOANGET  THEN
        v_Name := 'Займ привлечение';
      ELSE
        v_Name := 'Не определено';
      END IF;

      RETURN v_Name;
    END;

    -- Функция считывает актуальные значения настроек НУ из реестра и заносит их в глобальные структуры
    procedure GetSettingsTax
    is
    begin
       ReestrValue.W1 := Rsb_Common.GetRegIntValue('COMMON\НДФЛ\ПРОДАЖА БЛОКИР. ПРИОБРЕТЕНИЙ', 0);
       ReestrValue.W2 := Rsb_Common.GetRegIntValue('COMMON\НДФЛ\ПРОВОДИТЬ ПЕРЕТАСОВКУ ДЛЯ НДФЛ', 0);
       ReestrValue.W3 := Rsb_Common.GetRegBoolValue('COMMON\НДФЛ\УЧЕТ РЕПО ДЛЯ СРОКА НЕПР. ВЛАД.', 0);
    end; --GetSettingsTax

    FUNCTION DetermineIISCountingStatus(
        DocKind IN NUMBER,   -- Вид документа 
        DocID IN NUMBER,     -- Идентификатор документа
        pContract IN NUMBER DEFAULT 0  -- ID субдоговора (опционально, по умолчанию 0)
    ) RETURN CHAR DETERMINISTIC  -- Возвращает признак 'T_NOTCOUNTEDONIIS'
    IS
        v_NotCountedOnIIS CHAR := CHR(0);
    BEGIN
        -- Обработка документов типа DL_SECURITYDOC и DL_CONVAVR
        IF DocKind IN (RSI_NPTXC.DL_SECURITYDOC, RSI_NPTXC.DL_CONVAVR) AND pContract > 0 THEN
            -- Если договор не является ИИС (CheckContrIIS = 0), возвращаем признак 'X' (CHR(88))
            RETURN CASE WHEN RSI_NPTO.CheckContrIIS(pContract) = 0 THEN CHR(88) ELSE CHR(0) END;
        END IF;

        IF DocKind = RSI_NPTXC.DL_AVRWRT THEN
            BEGIN
                SELECT T_NOTCOUNTEDONIIS 
                INTO v_NotCountedOnIIS 
                FROM ddlsum_dbt 
                WHERE t_dockind = DocKind 
                  AND t_docid = DocID 
                  AND t_kind = RSI_NPTXC.DLSUM_KIND_COSTWRTTAX 
                  AND ROWNUM = 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_NotCountedOnIIS := CHR(0);
            END;
            RETURN v_NotCountedOnIIS;
        END IF;

        -- Для остальных случаев возвращаем CHR(0) (признак не установлен)
        RETURN CHR(0);
    END;

  ----Выполняет списание лота продажи SALELOT из сделок покупок и обратного Репо
    PROCEDURE LinkSale( v_SaleLot IN dnptxlot_dbt%ROWTYPE, pIIS IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0   ) 
    IS
      v_S          NUMBER;
      v_Buy_ID     NUMBER;
      v_FreeAmount NUMBER;
      v_Buy_Kind   NUMBER;
      v_stat       NUMBER;
      v_A          NUMBER;
      v_Link_Type  NUMBER;
      v_FICODE     dfininstr_dbt.t_FI_Code%TYPE;
      v_ContrNUmber dsfcontr_dbt.t_Number%Type;
    BEGIN
      v_stat := 0;

      v_S := v_SaleLot.t_Amount;
      RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Вызов LinkSale, v_S = '||v_S);

      WHILE v_S > 0 AND v_stat = 0 LOOP
        BEGIN
          SELECT t_ID,     FreeAmount,   t_Kind
            INTO v_Buy_ID, v_FreeAmount, v_Buy_Kind
            FROM ( SELECT /*+ INDEX( Buy DNPTXLOT_DBT_IDXB)*/
                          Buy.t_ID,
                          (Buy.t_Amount - Buy.t_Sale) FreeAmount,
                          Buy.t_Kind
                     FROM dnptxlot_dbt Buy
                    WHERE Buy.t_Client = v_SaleLot.t_Client
                      AND RSI_NPTO.CheckContrIIS(Buy.t_Contract)  = pIIS 
                      AND ((pContract IS NULL) OR (pContract <= 0) OR (Buy.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                      AND Buy.t_FIID = v_SaleLot.t_FIID
                      AND Buy.t_IsFree = CHR(88)
                      AND Buy.t_BuyDate <= v_SaleLot.t_SaleDate
                 ORDER BY Buy.t_OrdForSale ASC,
                          Buy.t_BegBuyDate ASC,
                          Buy.t_DealDate ASC,
                          Buy.t_DealTime ASC,
                          NLSSORT(Buy.t_SortCode) ASC,
                          Buy.t_ID ASC)
           WHERE ROWNUM = 1;

          v_A := iif( v_S < v_FreeAmount, v_S, v_FreeAmount );
          IF v_Buy_Kind = RSI_NPTXC.NPTXLOTS_BUY THEN
             v_Link_Type := RSI_NPTXC.NPTXLNK_DELIVER;
          ELSE
             v_Link_Type := RSI_NPTXC.NPTXLNK_OPPOS;
          END IF;

          INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                     T_CONTRACT   ,
                                     T_FIID       ,
                                     T_BUYID      ,
                                     T_SALEID     ,
                                     T_TYPE       ,
                                     T_DATE       ,
                                     T_AMOUNT     )
                             VALUES( v_SaleLot.t_Client,      --T_CLIENT     
                                     v_SaleLot.t_Contract,    --T_CONTRACT   
                                     v_SaleLot.t_FIID,        --T_FIID       
                                     v_Buy_ID,                --T_BUYID
                                     v_SaleLot.t_ID,          --T_SALEID
                                     v_Link_Type,             --T_TYPE       
                                     v_SaleLot.t_SaleDate,    --T_DATE    
                                     v_A                      --T_AMOUNT     
                                   );

          RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'LinkSale: вставлена связь v_A = '||v_A);

          v_S := v_S - v_A;

        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              BEGIN
                v_stat := 1;

                SELECT t_FI_Code
                  INTO v_FICODE
                  FROM dfininstr_dbt
                 WHERE t_FIID = v_SaleLot.t_FIID;

                SELECT t_Number
                  INTO v_ContrNumber
                  FROM dsfcontr_dbt
                 WHERE t_ID = v_SaleLot.t_Contract;

                RSI_NPTMSG.PutMsgAutonom( RSI_NPTXC.MES_ERROR, v_ContrNumber||' Недостаточно ц/б "'||v_FICODE||'" на дату '||TO_CHAR(v_SaleLot.t_SaleDate,'DD.MM.YYYY')||
                                                               ' для списания лота продажи с внешним кодом "'||v_SaleLot.t_DealCodeTS||'"' );
                RSI_NPTO.SetError(RSI_NPTXC.NPTX_ERROR_20647, '');
              END;

        END;
      END LOOP;
    END; --LinkSale


  ----Выполняет списание лота выбытия прямого Репо/размещения займа SALELOT  из сделок покупок,  обратного Репо и привлечения займа
    PROCEDURE LinkDirectRepo( v_SaleLot IN dnptxlot_dbt%ROWTYPE, pIIS IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0  ) 
    IS
      v_S          NUMBER;
      v_Buy_ID     NUMBER;
      v_FreeAmount NUMBER;
      v_stat       NUMBER;
      v_A          NUMBER;
      v_FICODE     dfininstr_dbt.t_FI_Code%TYPE;
      v_ContrNumber dsfcontr_dbt.t_Number%Type;
    BEGIN
      v_stat := 0;

      v_S := v_SaleLot.t_Amount;

      WHILE v_S > 0 AND v_stat = 0 LOOP
        BEGIN
          if (ReestrValue.W2 = RSI_NPTXC.NPTXREG_W2_NO) then
             SELECT t_ID,     FreeAmount
               INTO v_Buy_ID, v_FreeAmount
               FROM ( SELECT /*+ INDEX( Buy DNPTXLOT_DBT_IDXC)*/
                             Buy.t_ID,
                             (Buy.t_Amount - Buy.t_Sale) FreeAmount,
                             Buy.t_Kind
                        FROM dnptxlot_dbt Buy
                       WHERE Buy.t_Client = v_SaleLot.t_Client
                         AND RSI_NPTO.CheckContrIIS(Buy.t_Contract)  = pIIS  
                         AND ((pContract IS NULL) OR (pContract <= 0) OR (Buy.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                         AND Buy.t_FIID = v_SaleLot.t_FIID
                         AND Buy.t_IsFree = CHR(88)
                         AND Buy.t_BuyDate <= v_SaleLot.t_SaleDate
                    ORDER BY Buy.t_OrdForRepo ASC,
                             Buy.t_BegBuyDate ASC,
                             Buy.t_DealDate ASC,
                             Buy.t_DealTime ASC,
                             NLSSORT(Buy.t_SortCode) ASC,
                             Buy.t_ID ASC)
              WHERE ROWNUM = 1;
          else
             SELECT t_ID,     FreeAmount
               INTO v_Buy_ID, v_FreeAmount
               FROM ( SELECT /*+ INDEX( Buy DNPTXLOT_DBT_IDXD)*/
                             Buy.t_ID,
                             (Buy.t_Amount - Buy.t_Sale) FreeAmount,
                             Buy.t_Kind
                        FROM dnptxlot_dbt Buy
                       WHERE Buy.t_Client = v_SaleLot.t_Client
                         AND RSI_NPTO.CheckContrIIS(Buy.t_Contract)  = pIIS 
                         AND ((pContract IS NULL) OR (pContract <= 0) OR (Buy.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                         AND Buy.t_FIID = v_SaleLot.t_FIID
                         AND Buy.t_IsFree = CHR(88)
                         AND Buy.t_BuyDate <= v_SaleLot.t_SaleDate
                    ORDER BY Buy.t_OrdForRepo ASC,
                             Buy.t_BegBuyDate DESC,
                             Buy.t_DealDate DESC,
                             Buy.t_DealTime DESC,
                             NLSSORT(Buy.t_SortCode) DESC,
                             Buy.t_ID DESC)
              WHERE ROWNUM = 1;
          end if;

          v_A := iif( v_S < v_FreeAmount, v_S, v_FreeAmount );

          INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                     T_CONTRACT   ,
                                     T_FIID       ,
                                     T_BUYID      ,
                                     T_SALEID     ,
                                     T_TYPE       ,
                                     T_DATE       ,
                                     T_AMOUNT     )
                             VALUES( v_SaleLot.t_Client,         --T_CLIENT     
                                     v_SaleLot.t_Contract,       --T_CONTRACT   
                                     v_SaleLot.t_FIID,           --T_FIID       
                                     v_Buy_ID,                   --T_BUYID
                                     v_SaleLot.t_ID,             --T_SALEID
                                     RSI_NPTXC.NPTXLNK_REPO,     --T_TYPE       
                                     v_SaleLot.t_SaleDate,       --T_DATE    
                                     v_A                         --T_AMOUNT     
                                   );

          v_S := v_S - v_A;
        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              BEGIN
                v_stat := 1;

                SELECT t_FI_Code
                  INTO v_FICODE
                  FROM dfininstr_dbt
                 WHERE t_FIID = v_SaleLot.t_FIID;

                SELECT t_Number
                  INTO v_ContrNumber
                  FROM dsfcontr_dbt
                 WHERE t_ID = v_SaleLot.t_Contract;

                RSI_NPTMSG.PutMsgAutonom( RSI_NPTXC.MES_ERROR, v_ContrNumber||' Недостаточно ц/б "'||v_FICODE||'" на дату '||TO_CHAR(v_SaleLot.t_SaleDate,'DD.MM.YYYY')||
                                                               ' для списания лота вида "'||get_lotName(v_SaleLot.t_Kind)||'" с внешним кодом "'||v_SaleLot.t_DealCodeTS||'"' );
                RSI_NPTO.SetError(RSI_NPTXC.NPTX_ERROR_20647, ''); 
              END;

        END;
      END LOOP;
    END; --LinkDirectRepo

  ----Выполняет связывание по 2 ч. лота Репо обратного/Займу размещения. PART2LOT
    PROCEDURE LinkPart2ToBuy( v_Part2Lot IN dnptxlot_dbt%ROWTYPE, pIIS IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0  )
    IS
      v_S           NUMBER;
      v_SPS         NUMBER;
      v_FreeAmount  NUMBER;
      v_BuyLot_Kind NUMBER;
      v_BuyLot_ID   NUMBER;
      v_A           NUMBER;
      v_VirtualNum  NUMBER;
      v_Break       BOOLEAN;
      v_Lnk_A       NUMBER;
      v_Lnk_S       NUMBER;
      v_Lnk_ID      NUMBER;
      v_SaleLotID   NUMBER;
      v_SaleType    NUMBER;
      v_SaleRID     NUMBER;
      v_RealID      NUMBER;
      v_PriceFIID   NUMBER;
      v_Price       NUMBER;
      v_NKD         NUMBER;
      v_TotalCost   NUMBER;
      v_LotDealCode dnptxlot_dbt.t_DealCode%TYPE;
      v_VB_ID       NUMBER;
      v_VS_ID       NUMBER;
      v_LCS_ID      NUMBER;
      v_SaleLotKind NUMBER;
      v_SaleBegLotID NUMBER;
      v_SalePriceFIID   NUMBER;
      v_SalePrice       NUMBER;
      v_SaleNKD         NUMBER;
      v_SaleTotalCost   NUMBER;
      v_SaleDealDate    DATE;
      v_SaleDealTime    dnptxlot_dbt.t_DealTime%TYPE;
      v_SaleBegBuyDate  DATE;
      v_SaleBegSaleDate DATE; 
      v_SaleSortCode    dnptxlot_dbt.t_SortCode%TYPE;
      v_DealDate        DATE;
      v_DealTime        dnptxlot_dbt.t_DealTime%TYPE;
      v_BegBuyDate      DATE;
      v_BegSaleDate     DATE; 
      v_SortCode        dnptxlot_dbt.t_SortCode%TYPE;
      v_Type        NUMBER;
      v_SaleLotDealCodeTS dnptxlot_dbt.t_DealCodeTS%TYPE;
      v_FICODE     dfininstr_dbt.t_FI_Code%TYPE;
      v_ContrNumber dsfcontr_dbt.t_Number%Type;

      TYPE OSPLNKCurTyp IS REF CURSOR;
      OSPLNK_cur OSPLNKCurTyp;

    BEGIN
      v_S := v_Part2Lot.t_Sale;
      RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Вызов LinkPart2ToBuy, v_S = '||v_S);

      IF v_S = 0 THEN
        RETURN; -- нет списаний
      END IF;

      --Принудительное закрытие коротких позиций
      OPEN OSPLNK_cur FOR SELECT lnk.t_Amount, lnk.t_Short, lnk.t_ID LinkID,
                                 salelot.t_ID, salelot.t_Type,
                                 salelot.t_RealID, salelot.t_DealCodeTS, salelot.t_BegLotID,
                                 salelot.t_PriceFIID, salelot.t_Price, salelot.t_NKD, salelot.t_TotalCost,
                                 salelot.t_DealDate, salelot.t_DealTime, salelot.t_BegBuyDate, 
                                 salelot.t_BegSaleDate, salelot.t_SortCode 
                            FROM dnptxlnk_dbt lnk, dnptxlot_dbt salelot
                           WHERE lnk.t_BuyID = v_Part2Lot.t_ID
                             AND lnk.t_Type = RSI_NPTXC.NPTXLNK_OPPOS
                             AND salelot.t_ID = lnk.t_SaleID
                             AND (lnk.t_Amount - lnk.t_Short) > 0
                        ORDER BY salelot.t_BegSaleDate ASC,
                                 salelot.t_DealDate ASC,
                                 salelot.t_DealTime ASC,
                                 NLSSORT(salelot.t_SortCode) ASC,
                                 salelot.t_ID ASC;
      LOOP
        FETCH OSPLNK_cur INTO v_Lnk_A, v_Lnk_S, v_Lnk_ID,
                              v_SaleLotID, v_SaleType,
                              v_SaleRID, v_SaleLotDealCodeTS, v_SaleBegLotID,
                              v_SalePriceFIID, v_SalePrice, v_SaleNKD, v_SaleTotalCost,
                              v_SaleDealDate, v_SaleDealTime, v_SaleBegBuyDate, 
                              v_SaleBegSaleDate, v_SaleSortCode;

        EXIT WHEN OSPLNK_cur%NOTFOUND OR
                  OSPLNK_cur%NOTFOUND IS NULL;

        BEGIN

          v_SPS := v_Lnk_A  - v_Lnk_S;
          v_Break := FALSE;

          WHILE v_SPS > 0 LOOP
            BEGIN
              SELECT *
                INTO v_BuyLot_ID, v_BuyLot_Kind, v_FreeAmount
                FROM ( SELECT /*+ INDEX (Buy DNPTXLOT_DBT_IDXB)*/
                              Buy.t_ID,
                              Buy.t_Kind,
                              (Buy.t_Amount - Buy.t_Sale) FreeAmount
                         FROM dnptxlot_dbt Buy
                        WHERE Buy.t_Client = v_Part2Lot.t_Client
                          AND RSI_NPTO.CheckContrIIS(Buy.t_Contract)  = pIIS 
                          AND ((pContract IS NULL) OR (pContract <= 0) OR (Buy.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                          AND Buy.t_FIID = v_Part2Lot.t_FIID
                          AND Buy.t_BuyDate <= v_Part2Lot.t_SaleDate
                          AND Buy.t_IsFree = CHR(88)
                          AND Buy.t_ID <> v_Part2Lot.t_ID
                          AND ((v_Part2Lot.t_SaleDate  > RSI_NPTXC.NPTX_ENDDATE2011 AND
                                Buy.t_Kind IN (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET)
                               ) OR
                               (v_Part2Lot.t_SaleDate <= RSI_NPTXC.NPTX_ENDDATE2011 
                               )
                              )
                     ORDER BY Buy.t_OrdForSale ASC,
                              Buy.t_BegBuyDate ASC,
                              Buy.t_DealDate ASC,
                              Buy.t_DealTime ASC,
                              NLSSORT(Buy.t_SortCode) ASC,
                              Buy.t_ID ASC )
               WHERE ROWNUM = 1;

            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                BEGIN
                  v_Break := TRUE;

                  SELECT t_FI_Code
                    INTO v_FICODE
                    FROM dfininstr_dbt
                   WHERE t_FIID = v_Part2Lot.t_FIID;

                  SELECT t_Number
                    INTO v_ContrNumber
                    FROM dsfcontr_dbt
                   WHERE t_ID = v_Part2Lot.t_Contract;

                  RSI_NPTMSG.PutMsgAutonom( RSI_NPTXC.MES_ERROR, v_ContrNumber||' Недостаточно ц/б "'||v_FICODE||'" на дату '||TO_CHAR(v_Part2Lot.t_SaleDate,'DD.MM.YYYY')||
                                                                 ' для закрытия короткой позиции по сделке вида "'||get_lotName(v_Part2Lot.t_Kind)||'" с внешним кодом "'||v_Part2Lot.t_DealCodeTS||
                                                                 '", открытой продажей с внешним кодом "'||v_SaleLotDealCodeTS||'"' );
                  RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20647, '');
                END;
            END;

            EXIT WHEN v_Break;

            v_A := iif( v_SPS < v_FreeAmount, v_SPS, v_FreeAmount );

            IF v_BuyLot_Kind = RSI_NPTXC.NPTXLOTS_BUY THEN
               INSERT INTO dnptxlnk_dbt( T_CLIENT     ,
                                         T_CONTRACT   ,
                                         T_FIID       ,
                                         T_BUYID      ,
                                         T_SALEID     ,
                                         T_SOURCEID   ,
                                         T_TYPE       ,
                                         T_DATE       ,
                                         T_AMOUNT     )
                                 VALUES( v_Part2Lot.t_Client,         --T_CLIENT     
                                         v_Part2Lot.t_Contract,       --T_CONTRACT   
                                         v_Part2Lot.t_FIID,           --T_FIID       
                                         v_BuyLot_ID,                 --T_BUYID
                                         v_SaleLotID,                 --T_SALEID
                                         v_Part2Lot.t_ID,             --T_SOURCEID
                                         RSI_NPTXC.NPTXLNK_CLPOS,     --T_TYPE       
                                         v_Part2Lot.t_SaleDate,       --T_DATE    
                                         v_A                          --T_AMOUNT     
                                       ) RETURNING t_ID INTO v_LCS_ID;

            ELSE --Обратное Репо или привлечение займа

              SELECT NVL(MAX(GetVirtCountByNum(t_DealCodeTS)), 0) + 1
                INTO v_VirtualNum
                FROM dnptxlot_dbt
               WHERE ( (t_SaleDate = v_Part2Lot.t_SaleDate AND t_Kind = RSI_NPTXC.NPTXLOTS_SALE) OR
                       (t_BuyDate = v_Part2Lot.t_SaleDate AND t_Kind = RSI_NPTXC.NPTXLOTS_BUY) )
                 AND (t_Type = RSI_NPTXC.NPTXDEAL_MARKET OR
                      t_Type = RSI_NPTXC.NPTXDEAL_CALC  
                     );

              IF v_SaleType IN (RSI_NPTXC.NPTXDEAL_REAL, RSI_NPTXC.NPTXDEAL_COMP) THEN
                 v_RealID := v_SaleLotID;
              ELSE 
                 v_RealID := v_SaleRID;
              END IF;

              if (v_Part2Lot.t_SaleDate <= RSI_NPTXC.NPTX_ENDDATE2011) then
                 v_PriceFIID := GetFaceValueFI (v_Part2Lot.t_FIID);
                 v_Price := 0;
                 v_NKD := GetNKDCource (v_Part2Lot.t_FIID, v_Part2Lot.t_SaleDate) * v_A; 
                 v_TotalCost := 0;
              else
                 v_PriceFIID := v_SalePriceFIID;
                 v_Price := v_SalePrice;
                 v_NKD := v_SaleNKD; 
                 v_TotalCost := v_SaleTotalCost;
              end if;

              if (v_Part2Lot.t_SaleDate <= RSI_NPTXC.NPTX_ENDDATE2011) then
                 v_Type := RSI_NPTXC.NPTXDEAL_MARKET;
                 v_DealDate := v_Part2Lot.t_SaleDate;
                 v_DealTime := TO_DATE('01.01.0001 09:00:00','DD.MM.YYYY HH24:MI:SS');

                 v_BegBuyDate  := TO_DATE('01.01.0001','DD.MM.YYYY');
                 v_BegSaleDate := TO_DATE('01.01.0001','DD.MM.YYYY');
                 v_SortCode    := CHR(1);
              else
                 v_Type := RSI_NPTXC.NPTXDEAL_CALC;
                 v_DealDate := v_SaleDealDate;
                 v_DealTime := v_SaleDealTime;

                 v_BegBuyDate  := v_SaleBegBuyDate;
                 v_BegSaleDate := TO_DATE('01.01.0001','DD.MM.YYYY');
                 v_SortCode    := v_SaleSortCode;
              end if;

              v_LotDealCode := GenVirtNum( RSI_NPTXC.NPTXLOTS_BUY, v_Type, v_Part2Lot.t_SaleDate, v_VirtualNum );

              INSERT INTO dnptxlot_dbt ( T_DEALDATE   ,
                                         T_DEALTIME   ,
                                         T_DEALCODE   ,
                                         T_DEALCODETS ,
                                         T_CLIENT     ,
                                         T_CONTRACT   ,
                                         T_FIID       ,
                                         T_KIND       ,
                                         T_TYPE       ,
                                         T_BEGLOTID   ,
                                         T_REALID     ,
                                         T_AMOUNT     ,
                                         T_PRICE      ,
                                         T_PRICEFIID  ,
                                         T_TOTALCOST  ,
                                         T_NKD        ,
                                         T_BUYDATE    ,
                                         T_SALEDATE   ,
                                         T_BEGBUYDATE ,
                                         T_BEGSALEDATE,   
                                         T_SORTCODE   
                                       )
                                 VALUES( v_DealDate,                           --T_DEALDATE
                                         v_DealTime,                           --T_DEALTIME
                                         v_LotDealCode,                        --T_DEALCODE
                                         v_LotDealCode,                        --T_DEALCODETS
                                         v_Part2Lot.t_Client,                  --T_CLIENT     
                                         v_Part2Lot.t_Contract,                --T_CONTRACT   
                                         v_Part2Lot.t_FIID,                    --T_FIID       
                                         RSI_NPTXC.NPTXLOTS_BUY,               --T_KIND       
                                         v_Type,                               --T_TYPE       
                                         v_SaleBegLotID,                       --T_BEGLOTID
                                         v_RealID,                             --T_REALID
                                         v_A,                                  --T_AMOUNT     
                                         v_Price,                              --T_PRICE    
                                         v_PriceFIID,                          --T_PRICEFIID
                                         v_TotalCost,                          --T_TOTALCOST
                                         v_NKD,                                --T_NKD      
                                         v_Part2Lot.t_SaleDate,                --T_BUYDATE    
                                         TO_DATE('01.01.0001','DD.MM.YYYY'),   --T_SALEDATE   
                                         v_BegBuyDate,                         --T_BEGBUYDATE 
                                         v_BegSaleDate,                        --T_BEGSALEDATE
                                         v_SortCode                            --T_SORTCODE   
                                       ) RETURNING t_ID INTO v_VB_ID;

              if (v_Part2Lot.t_SaleDate <= RSI_NPTXC.NPTX_ENDDATE2011) then
                 v_Price := RSI_NPTO.GetMarketPrice (v_Part2Lot.t_FIID, v_Part2Lot.t_SaleDate, -1, v_VB_ID);
                 v_TotalCost := v_Price * v_A + v_NKD;

                 UPDATE dnptxlot_dbt
                    SET t_Price     = v_Price,   
                        t_TotalCost = v_TotalCost
                  WHERE t_ID = v_VB_ID;
              end if;

              if (v_Part2Lot.t_SaleDate <= RSI_NPTXC.NPTX_ENDDATE2011) then
                 v_BegBuyDate  := TO_DATE('01.01.0001','DD.MM.YYYY');
                 v_BegSaleDate := TO_DATE('01.01.0001','DD.MM.YYYY');
                 v_SortCode    := CHR(1);
              else
                 v_BegBuyDate  := TO_DATE('01.01.0001','DD.MM.YYYY');
                 v_BegSaleDate := v_SaleBegSaleDate;
                 v_SortCode    := v_SaleSortCode;
              end if;

              v_LotDealCode := GenVirtNum( RSI_NPTXC.NPTXLOTS_SALE, v_Type, v_Part2Lot.t_SaleDate, v_VirtualNum );

              INSERT INTO dnptxlot_dbt ( T_DEALDATE   ,
                                         T_DEALTIME   ,
                                         T_DEALCODE   ,
                                         T_DEALCODETS ,
                                         T_CLIENT     ,
                                         T_CONTRACT   ,
                                         T_FIID       ,
                                         T_KIND       ,
                                         T_TYPE       ,
                                         T_BEGLOTID   ,
                                         T_BUYID      ,
                                         T_REALID     ,
                                         T_AMOUNT     ,
                                         T_PRICE      ,
                                         T_PRICEFIID  ,
                                         T_TOTALCOST  ,
                                         T_NKD        ,
                                         T_BUYDATE    ,
                                         T_SALEDATE   ,
                                         T_BEGBUYDATE ,
                                         T_BEGSALEDATE,   
                                         T_SORTCODE   
                                       )
                                 VALUES( v_DealDate,                           --T_DEALDATE
                                         v_DealTime,                           --T_DEALTIME
                                         v_LotDealCode,                        --T_DEALCODE
                                         v_LotDealCode,                        --T_DEALCODETS
                                         v_Part2Lot.t_Client,                  --T_CLIENT     
                                         v_Part2Lot.t_Contract,                --T_CONTRACT   
                                         v_Part2Lot.t_FIID,                    --T_FIID       
                                         RSI_NPTXC.NPTXLOTS_SALE,              --T_KIND       
                                         v_Type,                               --T_TYPE       
                                         v_SaleBegLotID,                       --T_BEGLOTID
                                         v_VB_ID,                              --T_BUYID
                                         v_RealID,                             --T_REALID
                                         v_A,                                  --T_AMOUNT     
                                         v_Price,                              --T_PRICE    
                                         v_PriceFIID,                          --T_PRICEFIID
                                         v_TotalCost,                          --T_TOTALCOST
                                         v_NKD,                                --T_NKD      
                                         TO_DATE('01.01.0001','DD.MM.YYYY'),   --T_BUYDATE    
                                         v_Part2Lot.t_SaleDate,                --T_SALEDATE   
                                         v_BegBuyDate,                         --T_BEGBUYDATE 
                                         v_BegSaleDate,                        --T_BEGSALEDATE
                                         v_SortCode                            --T_SORTCODE   
                                       ) RETURNING t_ID INTO v_VS_ID;

              INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                         T_CONTRACT   ,
                                         T_FIID       ,
                                         T_BUYID      ,
                                         T_SALEID     ,
                                         T_SOURCEID   ,
                                         T_TYPE       ,
                                         T_DATE       ,
                                         T_AMOUNT     )
                                 VALUES( v_Part2Lot.t_Client,      --T_CLIENT     
                                         v_Part2Lot.t_Contract,    --T_CONTRACT   
                                         v_Part2Lot.t_FIID,        --T_FIID       
                                         v_VB_ID,                  --T_BUYID
                                         v_SaleLotID,              --T_SALEID
                                         v_Part2Lot.t_ID,          --T_SOURCEID
                                         RSI_NPTXC.NPTXLNK_CLPOS,  --T_TYPE       
                                         v_Part2Lot.t_SaleDate,    --T_DATE    
                                         v_A                       --T_AMOUNT     
                                       ) RETURNING t_ID INTO v_LCS_ID;

              INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                         T_CONTRACT   ,
                                         T_FIID       ,
                                         T_BUYID      ,
                                         T_SALEID     ,
                                         T_SOURCEID   ,
                                         T_TYPE       ,
                                         T_DATE       ,
                                         T_AMOUNT     )
                                 VALUES( v_Part2Lot.t_Client,      --T_CLIENT     
                                         v_Part2Lot.t_Contract,    --T_CONTRACT   
                                         v_Part2Lot.t_FIID,        --T_FIID       
                                         v_BuyLot_ID,              --T_BUYID
                                         v_VS_ID,                  --T_SALEID
                                         0,                        --T_SOURCEID
                                         RSI_NPTXC.NPTXLNK_OPPOS,  --T_TYPE       
                                         v_Part2Lot.t_SaleDate,    --T_DATE    
                                         v_A                       --T_AMOUNT     
                                       );
            END IF;

            INSERT INTO dnptxls_dbt( t_ChildID,
                                     t_ParentID,
                                     t_Short)
                             VALUES( v_LCS_ID,
                                     v_Lnk_ID,
                                     v_A );
            v_S := v_S - v_A;
            v_SPS := v_SPS - v_A;

          END LOOP;
        END;
      END LOOP;

      CLOSE OSPLNK_cur;

      IF( v_S = 0 ) THEN
        RETURN; -- все связали, выходим
      END IF;

      -- формируем связи по подстановкам
      OPEN OSPLNK_cur FOR SELECT lnk.t_Amount - lnk.t_Short, salelot.t_ID, salelot.t_Kind, salelot.t_DealCodeTS, lnk.t_ID
                            FROM dnptxlnk_dbt lnk, dnptxlot_dbt salelot
                           WHERE lnk.t_BuyID = v_Part2Lot.t_ID
                             AND lnk.t_Type IN (RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO)
                             AND salelot.t_ID = lnk.t_SaleID
                             AND (lnk.t_Amount - lnk.t_Short) > 0
                             AND (salelot.t_BuyDate > v_Part2Lot.t_SaleDate OR salelot.t_BuyDate = TO_DATE('01-01-0001','DD-MM-YYYY'))
                        ORDER BY salelot.t_BegSaleDate ASC,
                                 salelot.t_DealDate ASC,
                                 salelot.t_DealTime ASC,
                                 NLSSORT(salelot.t_SortCode) ASC,
                                 salelot.t_ID ASC;
      LOOP

        FETCH OSPLNK_cur INTO v_SPS, v_SaleLotID, v_SaleLotKind, v_SaleLotDealCodeTS, v_Lnk_ID;
        EXIT WHEN OSPLNK_cur%NOTFOUND OR
                  OSPLNK_cur%NOTFOUND IS NULL;

        v_Break := FALSE;

        WHILE v_SPS > 0 LOOP
          BEGIN
            if (v_Part2Lot.t_SaleDate <= RSI_NPTXC.NPTX_ENDDATE2011 OR ReestrValue.W2 = RSI_NPTXC.NPTXREG_W2_NO) then
               SELECT *
                 INTO v_BuyLot_ID, v_BuyLot_Kind, v_FreeAmount
                 FROM ( SELECT /*+ INDEX(Buy DNPTXLOT_DBT_IDXC)*/
                               Buy.t_ID,
                               Buy.t_Kind,
                               (Buy.t_Amount - Buy.t_Sale) FreeAmount
                          FROM dnptxlot_dbt Buy
                         WHERE Buy.t_Client = v_Part2Lot.t_Client
                           AND RSI_NPTO.CheckContrIIS(Buy.t_Contract)  = pIIS 
                           AND ((pContract IS NULL) OR (pContract <= 0) OR (Buy.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                           AND Buy.t_FIID = v_Part2Lot.t_FIID
                           AND Buy.t_BuyDate <= v_Part2Lot.t_SaleDate
                           AND Buy.t_IsFree = CHR(88)
                           AND Buy.t_ID <> v_Part2Lot.t_ID
                      ORDER BY Buy.t_OrdForRepo ASC,
                               Buy.t_BegBuyDate ASC,
                               Buy.t_DealDate ASC,
                               Buy.t_DealTime ASC,
                               NLSSORT(Buy.t_SortCode) ASC,
                               Buy.t_ID ASC )
                WHERE ROWNUM = 1;
            else
               SELECT *
                 INTO v_BuyLot_ID, v_BuyLot_Kind, v_FreeAmount
                 FROM ( SELECT /*+ INDEX(Buy DNPTXLOT_DBT_IDXD)*/
                               Buy.t_ID,
                               Buy.t_Kind,
                               (Buy.t_Amount - Buy.t_Sale) FreeAmount
                          FROM dnptxlot_dbt Buy
                         WHERE Buy.t_Client = v_Part2Lot.t_Client
                           AND RSI_NPTO.CheckContrIIS(Buy.t_Contract)  = pIIS 
                           AND ((pContract IS NULL) OR (pContract <= 0) OR (Buy.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                           AND Buy.t_FIID = v_Part2Lot.t_FIID
                           AND Buy.t_BuyDate <= v_Part2Lot.t_SaleDate
                           AND Buy.t_IsFree = CHR(88)
                           AND Buy.t_ID <> v_Part2Lot.t_ID
                      ORDER BY Buy.t_OrdForRepo ASC,
                               Buy.t_BegBuyDate DESC,
                               Buy.t_DealDate DESC,
                               Buy.t_DealTime DESC,
                               NLSSORT(Buy.t_SortCode) DESC,
                               Buy.t_ID DESC )
                WHERE ROWNUM = 1;
            end if;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                v_Break := TRUE;

                SELECT t_FI_Code
                  INTO v_FICODE
                  FROM dfininstr_dbt
                 WHERE t_FIID = v_Part2Lot.t_FIID;

                SELECT t_Number
                  INTO v_ContrNumber
                  FROM dsfcontr_dbt
                 WHERE t_ID = v_Part2Lot.t_Contract;

                RSI_NPTMSG.PutMsgAutonom( RSI_NPTXC.MES_ERROR, v_ContrNumber||' Недостаточно ц/б "'||v_FICODE||'" на дату '||TO_CHAR(v_Part2Lot.t_SaleDate,'DD.MM.YYYY')||
                                                               ' для переноса участия по сделке вида "'||get_lotName(v_SaleLotKind)||'" с внешним кодом "'||v_SaleLotDealCodeTS||
                                                               '" при выбытии 2ч. сделки вида "'||get_lotName(v_Part2Lot.t_Kind)||'" с внешним кодом "'||v_Part2Lot.t_DealCodeTS||'"' );
                RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20647, '');
              END;
          END;

          EXIT WHEN v_Break;

          v_A := iif( v_SPS < v_FreeAmount, v_SPS, v_FreeAmount );

          BEGIN
            SELECT t_ID
              INTO v_LCS_ID
              FROM dnptxlnk_dbt
             WHERE t_Type = RSI_NPTXC.NPTXLNK_SUBSTREPO
               AND t_BuyID = v_BuyLot_ID
               AND t_SaleID = v_SaleLotID
               AND t_SourceID = v_Part2Lot.t_ID
               AND t_DATE = v_Part2Lot.t_SaleDate;

            UPDATE dnptxlnk_dbt
               SET t_Amount = t_Amount + v_A
             WHERE t_ID = v_LCS_ID;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN

                INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                           T_CONTRACT   ,
                                           T_FIID       ,
                                           T_BUYID      ,
                                           T_SALEID     ,
                                           T_SOURCEID   ,
                                           T_TYPE       ,
                                           T_DATE       ,
                                           T_AMOUNT     )
                                   VALUES( v_Part2Lot.t_Client,         --T_CLIENT     
                                           v_Part2Lot.t_Contract,       --T_CONTRACT   
                                           v_Part2Lot.t_FIID,           --T_FIID       
                                           v_BuyLot_ID,                 --T_BUYID
                                           v_SaleLotID,                 --T_SALEID
                                           v_Part2Lot.t_ID,             --T_SOURCEID
                                           RSI_NPTXC.NPTXLNK_SUBSTREPO, --T_TYPE       
                                           v_Part2Lot.t_SaleDate,       --T_DATE    
                                           v_A                          --T_AMOUNT     
                                         ) RETURNING t_ID INTO v_LCS_ID;
              END;
          END;

          INSERT INTO dnptxls_dbt( t_ChildID,
                                   t_ParentID,
                                   t_Short)
                           VALUES( v_LCS_ID,
                                   v_Lnk_ID,
                                   v_A );
          v_SPS := v_SPS - v_A;
        END LOOP;

      END LOOP;

      CLOSE OSPLNK_cur;
    END; --LinkPart2ToBuy

  ----Выполняет закрытие незакрытых коротких позиций
    PROCEDURE CloseShortPos( in_CloseDate IN DATE, in_Client IN NUMBER, in_FIID IN NUMBER, pIIS IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0  )  
    IS
      v_Break       BOOLEAN;
      v_FIID        NUMBER;
      v_LinkID      NUMBER;
      v_SPS         NUMBER;
      v_SaleLotID   NUMBER;
      v_lotID       NUMBER;
      v_BuyLot_ID   NUMBER;
      v_FreeAmount  NUMBER;
      v_A           NUMBER;
      v_LCS_ID      NUMBER;
      v_Client      NUMBER;
      v_Contract    NUMBER;

      TYPE OSPLNKCurTyp IS REF CURSOR;
      OSPLNK_cur OSPLNKCurTyp;

    BEGIN

      OPEN OSPLNK_cur FOR SELECT (lnk.t_Amount - lnk.t_Short) SPS, salelot.t_FIID FIID, lnk.t_ID LinkID,
                                 salelot.t_ID, lot.t_ID, lot.t_Client, lot.t_Contract
                            FROM dnptxlnk_dbt lnk, dnptxlot_dbt salelot, dnptxlot_dbt lot
                           WHERE lnk.t_Type = RSI_NPTXC.NPTXLNK_OPPOS
                             AND salelot.t_ID = lnk.t_SaleID
                             AND lot.t_ID = lnk.t_BuyID
                             AND (lnk.t_Amount - lnk.t_Short) > 0
                             AND salelot.t_Client    = in_Client
                             AND RSI_NPTO.CheckContrIIS(salelot.t_Contract)  = pIIS 
                             AND ((pContract IS NULL) OR (pContract <= 0) OR (salelot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                             AND salelot.t_FIID      = in_FIID
                             AND salelot.t_SaleDate <= in_CloseDate
                        ORDER BY salelot.t_BegSaleDate ASC,
                                 salelot.t_DealDate ASC,
                                 salelot.t_DealTime ASC,
                                 NLSSORT(salelot.t_SortCode) ASC,
                                 salelot.t_ID ASC,
                                 lot.t_BegBuyDate ASC,
                                 lot.t_DealDate ASC,
                                 lot.t_DealTime ASC,
                                 NLSSORT(lot.t_SortCode) ASC,
                                 lot.t_ID ASC;
      LOOP

        FETCH OSPLNK_cur INTO v_SPS, v_FIID, v_LinkID, v_SaleLotID, v_lotID, v_Client, v_Contract;
        EXIT WHEN OSPLNK_cur%NOTFOUND OR
                  OSPLNK_cur%NOTFOUND IS NULL;

        v_Break := FALSE;

        WHILE v_SPS > 0 LOOP
          BEGIN
            SELECT *
              INTO v_BuyLot_ID, v_FreeAmount
              FROM ( SELECT /*+ INDEX (Buy DNPTXLOT_DBT_IDXB)*/
                            Buy.t_ID,
                            (Buy.t_Amount - Buy.t_Sale) FreeAmount
                       FROM dnptxlot_dbt Buy
                      WHERE Buy.t_Client = in_Client
                        AND RSI_NPTO.CheckContrIIS(Buy.t_Contract)  = pIIS  
                        AND ((pContract IS NULL) OR (pContract <= 0) OR (Buy.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                        AND Buy.t_FIID = v_FIID
                        AND Buy.t_BuyDate <= in_CloseDate
                        AND Buy.t_IsFree = CHR(88)
                        AND Buy.t_OrdForSale = 1 -- покупка
                   ORDER BY Buy.t_BegBuyDate ASC,
                            Buy.t_DealDate ASC,
                            Buy.t_DealTime ASC,
                            NLSSORT(Buy.t_SortCode) ASC,
                            Buy.t_ID ASC )
             WHERE ROWNUM = 1;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                v_Break := TRUE;
              END;
          END;

          EXIT WHEN v_Break;

          v_A := iif( v_SPS < v_FreeAmount, v_SPS, v_FreeAmount );

          INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                     T_CONTRACT   ,
                                     T_FIID       ,
                                     T_BUYID      ,
                                     T_SALEID     ,
                                     T_SOURCEID   ,
                                     T_TYPE       ,
                                     T_DATE       ,
                                     T_AMOUNT     )
                             VALUES( v_Client,                    --T_CLIENT     
                                     v_Contract,                  --T_CONTRACT   
                                     v_FIID,                      --T_FIID       
                                     v_BuyLot_ID,                 --T_BUYID
                                     v_SaleLotID,                 --T_SALEID
                                     v_lotID,                     --T_SOURCEID
                                     RSI_NPTXC.NPTXLNK_CLPOS,     --T_TYPE       
                                     in_CloseDate,                --T_DATE    
                                     v_A                          --T_AMOUNT     
                                   ) RETURNING t_ID INTO v_LCS_ID;

          INSERT INTO dnptxls_dbt( t_ChildID,
                                   t_ParentID,
                                   t_Short)
                           VALUES( v_LCS_ID,
                                   v_LinkID,
                                   v_A );
          v_SPS := v_SPS - v_A;

        END LOOP;

      END LOOP;

      CLOSE OSPLNK_cur;

    END; --CloseShortPos


  ----Выполняет обработку лота компенсационного платежа прямого Репо/займа размещения
    PROCEDURE ProcessCompPayOnDirectRepo( v_CLot IN dnptxlot_dbt%ROWTYPE )
    IS
      v_RLotID     NUMBER;
      v_Amount     NUMBER;
      v_A          NUMBER;
      v_LnkAmount  NUMBER;
      v_Buy_ID     NUMBER;
      v_LnkID      NUMBER;
      TYPE LNKCurTyp IS REF CURSOR;
      LNK_cur LNKCurTyp;

    BEGIN

      RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Вызов ProcessCompPayOnDirectRepo, v_CLot.t_DealCode = '||v_CLot.t_DealCode);

      UPDATE dnptxlot_dbt
         SET t_BuyDate = v_CLot.t_SaleDate,
             t_RetFlag = CHR(88)
       WHERE t_ChildID = v_CLot.t_ID;

      SELECT MIN(t_ID) INTO v_RLotID
        FROM dnptxlot_dbt
       WHERE t_ChildID = v_CLot.t_ID;

      UPDATE dnptxlnk_dbt
         SET t_RetFlag = CHR(88)
       WHERE t_SaleID = v_RLotID
         AND t_Type in (RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO);

      UPDATE dnptxlot_dbt
         SET t_InAcc = CHR(88)
       WHERE t_ID = v_CLot.t_ID;

      v_Amount := v_CLot.t_Amount;

      OPEN LNK_cur FOR SELECT Lnk.t_BuyID, (Lnk.t_Amount-Lnk.t_Short) LnkAmount
                         FROM dnptxlot_dbt B, dnptxlnk_dbt Lnk
                    LEFT JOIN dnptxlot_dbt LS on Lnk.t_SourceID = LS.t_ID
                        WHERE Lnk.t_Type in (RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO)
                          AND B.t_ID = Lnk.t_BuyID
                          AND Lnk.t_SaleID = v_RLotID
                          AND (Lnk.t_Amount-Lnk.t_Short) > 0

                          AND (B.t_SaleDate = TO_DATE('01-01-0001','DD-MM-YYYY') or
                               B.t_SaleDate >= v_CLot.t_SaleDate
                              )
                     ORDER BY B.t_OrdForRepo ASC,
                              B.t_BegBuyDate DESC,
                              B.t_DealDate DESC,
                              B.t_DealTime DESC,
                              NLSSORT(B.t_SortCode) DESC,

                              (CASE WHEN Lnk.t_SourceID = 0 OR
                                         Lnk.t_SourceID IS NULL
                                    THEN 1
                                    ELSE 2
                                    END) ASC,
                              LS.t_BegBuyDate DESC,
                              LS.t_DealDate DESC,
                              LS.t_DealTime DESC,
                              NLSSORT(LS.t_SortCode) DESC;

      WHILE v_Amount > 0 LOOP

        FETCH LNK_cur INTO v_Buy_ID, v_LnkAmount;
        EXIT WHEN LNK_cur%NOTFOUND OR
                  LNK_cur%NOTFOUND IS NULL;

        v_A := iif( v_Amount < v_LnkAmount, v_Amount, v_LnkAmount );
        BEGIN
          SELECT t_ID
            INTO v_LnkID
            FROM dnptxlnk_dbt
           WHERE t_Type = RSI_NPTXC.NPTXLNK_REPO
             AND t_BuyID = v_Buy_ID
             AND t_SaleID = v_CLot.t_ID
             AND t_SourceID = 0
             AND t_DATE = v_CLot.t_SaleDate;

          UPDATE dnptxlnk_dbt
             SET t_Amount = t_Amount + v_A
           WHERE t_ID = v_LnkID;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            BEGIN

              INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                         T_CONTRACT   ,
                                         T_FIID       ,
                                         T_BUYID      ,
                                         T_SALEID     ,
                                         T_SOURCEID   ,
                                         T_TYPE       ,
                                         T_DATE       ,
                                         T_AMOUNT     )
                                 VALUES( v_CLot.t_Client,         --T_CLIENT     
                                         v_CLot.t_Contract,       --T_CONTRACT   
                                         v_CLot.t_FIID,           --T_FIID       
                                         v_Buy_ID,                --T_BUYID
                                         v_CLot.t_ID,             --T_SALEID
                                         0,                       --T_SOURCEID
                                         RSI_NPTXC.NPTXLNK_REPO,  --T_TYPE       
                                         v_CLot.t_SaleDate,       --T_DATE    
                                         v_A                      --T_AMOUNT     
                                       );
            END;
        END;

        v_Amount := v_Amount - v_A;
      END LOOP;

      CLOSE LNK_cur;

    END; --ProcessCompPayOnDirectRepo

  ----Выполняет обработку лота компенсационного платежа обратного Репо/займа привлечения
    PROCEDURE ProcessCompPayOnReverseRepo( v_CLot IN dnptxlot_dbt%ROWTYPE, pIIS IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0 )  
    IS
      v_RLotID      NUMBER;
      v_RLot        dnptxlot_dbt%ROWTYPE;
      v_S           NUMBER;
      v_SPS         NUMBER;
      v_A           NUMBER;
      v_VirtualNum  NUMBER;
      v_RealID      NUMBER;
      v_SaleLot     dnptxlot_dbt%ROWTYPE;
      v_VB_ID       NUMBER;
      v_VS_ID       NUMBER;
      v_Lnk_A       NUMBER;
      v_Lnk_S       NUMBER;
      v_Lnk_ID      NUMBER;
      v_SaleLotID   NUMBER;
      v_SaleType    NUMBER;
      v_SaleRID     NUMBER;
      v_stat        NUMBER;
      v_SaleLotType NUMBER;
      v_LCS_ID      NUMBER;
      v_LotDealCode dnptxlot_dbt.t_DealCode%TYPE;
      TYPE LNKCurTyp IS REF CURSOR;
      LNK_cur LNKCurTyp;

    BEGIN

      RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Вызов ProcessCompPayOnReverseRepo, v_CLot.t_ID = '||v_CLot.t_ID);
      v_stat := 0;

      SELECT MIN(t_ID) INTO v_RLotID
        FROM dnptxlot_dbt
       WHERE t_ChildID = v_CLot.t_ID;

      UPDATE dnptxlot_dbt
         SET t_SaleDate = v_CLot.t_BuyDate
       WHERE t_ID = v_RLotID;

      SELECT * INTO v_RLot
        FROM dnptxlot_dbt
       WHERE t_ID = v_RLotID;

      UPDATE dnptxlot_dbt
         SET t_InAcc = CHR(88)
       WHERE t_ID = v_CLot.t_ID;

      v_S := iif( v_RLot.t_Sale < v_CLot.t_Amount, v_RLot.t_Sale, v_CLot.t_Amount );

      IF( v_S = 0 ) THEN
        IF v_CLot.t_BuyDate > RSI_NPTXC.NPTX_ENDDATE2011 THEN
           UPDATE dnptxlot_dbt
              SET t_RetFlag  = CHR(88)
            WHERE t_ID = v_RLotID;
        END IF;

        RETURN; -- все связали, выходим
      END IF;

      --закрываем короткие позиции по продаже
      OPEN LNK_cur FOR SELECT lnk.t_Amount, lnk.t_Short, lnk.t_ID LinkID,
                              salelot.t_ID, salelot.t_Type, salelot.t_RealID
                         FROM dnptxlnk_dbt lnk, dnptxlot_dbt salelot
                        WHERE lnk.t_BuyID  = v_RLot.t_ID
                          AND lnk.t_Type   = RSI_NPTXC.NPTXLNK_OPPOS
                          AND lnk.t_SaleID = salelot.t_ID
                          AND (lnk.t_Amount - lnk.t_Short) > 0
                     ORDER BY salelot.t_BegSaleDate DESC,
                              salelot.t_DealDate DESC,
                              salelot.t_DealTime DESC,
                              NLSSORT(salelot.t_SortCode) DESC;

      WHILE v_S > 0 and v_stat = 0 LOOP
        FETCH LNK_cur INTO v_Lnk_A, v_Lnk_S, v_Lnk_ID, v_SaleLotID, v_SaleType, v_SaleRID;
        EXIT WHEN LNK_cur%NOTFOUND OR
                  LNK_cur%NOTFOUND IS NULL;

        BEGIN

          SELECT * INTO v_SaleLot
            FROM dnptxlot_dbt
           WHERE t_ID = v_SaleLotID;

          v_SPS := v_Lnk_A  - v_Lnk_S;

          v_A   := iif( v_SPS < v_S, v_SPS, v_S );

          SELECT NVL(MAX(GetVirtCountByNum(t_DealCodeTS)), 0) + 1
            INTO v_VirtualNum
            FROM dnptxlot_dbt
           WHERE ( (t_SaleDate = v_RLot.t_SaleDate AND t_Kind = RSI_NPTXC.NPTXLOTS_SALE) OR
                   (t_BuyDate  = v_RLot.t_SaleDate AND t_Kind = RSI_NPTXC.NPTXLOTS_BUY) )
             AND (t_Type = RSI_NPTXC.NPTXDEAL_MARKET OR
                  t_Type = RSI_NPTXC.NPTXDEAL_CALC  
                 );

          IF v_SaleType IN (RSI_NPTXC.NPTXDEAL_REAL, RSI_NPTXC.NPTXDEAL_COMP) THEN
             v_RealID := v_SaleLotID;
          ELSE
             v_RealID := v_SaleRID;
          END IF;

          v_LotDealCode := GenVirtNum( RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXDEAL_CALC, v_RLot.t_SaleDate, v_VirtualNum );

          INSERT INTO dnptxlot_dbt ( T_DEALDATE   ,
                                     T_DEALTIME   ,
                                     T_DEALCODE   ,
                                     T_DEALCODETS ,
                                     T_CLIENT     ,
                                     T_CONTRACT   ,
                                     T_FIID       ,
                                     T_KIND       ,
                                     T_TYPE       ,
                                     T_BEGLOTID   ,
                                     T_BUYID      ,
                                     T_REALID     ,
                                     T_AMOUNT     ,
                                     T_PRICE      ,
                                     T_PRICEFIID  ,
                                     T_TOTALCOST  ,
                                     T_NKD        ,
                                     T_BUYDATE    ,
                                     T_BEGBUYDATE ,
                                     T_BEGSALEDATE,
                                     T_SALEDATE   ,
                                     T_SORTCODE   )
                             VALUES( v_SaleLot.t_SaleDate,                --T_DEALDATE
                                     v_SaleLot.t_DealTime,                --T_DEALTIME
                                     v_LotDealCode,                       --T_DEALCODE
                                     v_LotDealCode,                       --T_DEALCODETS
                                     v_RLot.t_Client,                     --T_CLIENT     
                                     v_RLot.t_Contract,                   --T_CONTRACT   
                                     v_RLot.t_FIID,                       --T_FIID       
                                     RSI_NPTXC.NPTXLOTS_BUY,              --T_KIND       
                                     RSI_NPTXC.NPTXDEAL_CALC,             --T_TYPE       
                                     v_SaleLot.t_BegLotID,                --T_BEGLOTID
                                     0,                                   --T_BUYID   
                                     v_RealID,                            --T_REALID
                                     v_A,                                 --T_AMOUNT     
                                     v_SaleLot.t_Price,                   --T_PRICE    
                                     v_SaleLot.t_PriceFIID,               --T_PRICEFIID
                                     v_SaleLot.t_TotalCost,               --T_TOTALCOST
                                     v_SaleLot.t_NKD,                     --T_NKD      
                                     v_RLot.t_SaleDate,                   --T_BUYDATE    
                                     v_SaleLot.t_BegBuyDate,              --T_BEGBUYDATE
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),  --T_BEGSALEDATE   
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),  --T_SALEDATE   
                                     v_SaleLot.t_SortCode                 --T_SORTCODE
                                   ) RETURNING t_ID INTO v_VB_ID;

          v_LotDealCode := GenVirtNum( RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXDEAL_CALC, v_RLot.t_SaleDate, v_VirtualNum );

          INSERT INTO dnptxlot_dbt ( T_DEALDATE   ,
                                     T_DEALTIME   ,
                                     T_DEALCODE   ,
                                     T_DEALCODETS ,
                                     T_CLIENT     ,
                                     T_CONTRACT   ,
                                     T_FIID       ,
                                     T_KIND       ,
                                     T_TYPE       ,
                                     T_BEGLOTID   ,
                                     T_BUYID      ,
                                     T_REALID     ,
                                     T_AMOUNT     ,
                                     T_PRICE      ,
                                     T_PRICEFIID  ,
                                     T_TOTALCOST  ,
                                     T_NKD        ,
                                     T_BUYDATE    ,
                                     T_BEGBUYDATE ,
                                     T_BEGSALEDATE,
                                     T_SALEDATE   ,
                                     T_SORTCODE   )
                             VALUES( v_SaleLot.t_SaleDate,                --T_DEALDATE
                                     v_SaleLot.t_DealTime,                --T_DEALTIME
                                     v_LotDealCode,                       --T_DEALCODE
                                     v_LotDealCode,                       --T_DEALCODETS
                                     v_RLot.t_Client,                     --T_CLIENT     
                                     v_RLot.t_Contract,                   --T_CONTRACT   
                                     v_RLot.t_FIID,                       --T_FIID       
                                     RSI_NPTXC.NPTXLOTS_SALE,             --T_KIND       
                                     RSI_NPTXC.NPTXDEAL_CALC,             --T_TYPE       
                                     v_SaleLot.t_BegLotID,                --T_BEGLOTID
                                     v_VB_ID,                             --T_BUYID   
                                     v_RealID,                            --T_REALID
                                     v_A,                                 --T_AMOUNT     
                                     v_SaleLot.t_Price,                   --T_PRICE    
                                     v_SaleLot.t_PriceFIID,               --T_PRICEFIID
                                     v_SaleLot.t_TotalCost*v_A/v_SaleLot.t_Amount, --T_TOTALCOST
                                     v_SaleLot.t_NKD*v_A/v_SaleLot.t_Amount,--T_NKD      
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),  --T_BUYDATE    
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),  --T_BEGBUYDATE
                                     v_RLot.t_SaleDate,                   --T_BEGSALEDATE   
                                     v_SaleLot.t_SaleDate,                --T_SALEDATE   
                                     v_SaleLot.t_SortCode                 --T_SORTCODE
                                   ) RETURNING t_ID INTO v_VS_ID;

          INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                     T_CONTRACT   ,
                                     T_FIID       ,
                                     T_BUYID      ,
                                     T_SALEID     ,
                                     T_SOURCEID   ,
                                     T_TYPE       ,
                                     T_DATE       ,
                                     T_AMOUNT     )
                             VALUES( v_RLot.t_Client,         --T_CLIENT     
                                     v_RLot.t_Contract,       --T_CONTRACT   
                                     v_RLot.t_FIID,           --T_FIID       
                                     v_VB_ID,                 --T_BUYID
                                     v_SaleLotID,             --T_SALEID
                                     v_RLot.t_ID,             --T_SOURCEID      
                                     RSI_NPTXC.NPTXLNK_CLPOS, --T_TYPE       
                                     v_RLot.t_SaleDate,       --T_DATE    
                                     v_A                      --T_AMOUNT     
                                   ) RETURNING t_ID INTO v_LCS_ID;

          INSERT INTO dnptxls_dbt( t_ChildID,
                                   t_ParentID,
                                   t_Short)
                           VALUES( v_LCS_ID,
                                   v_Lnk_ID,
                                   v_A );

          INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                     T_CONTRACT   ,
                                     T_FIID       ,
                                     T_BUYID      ,
                                     T_SALEID     ,
                                     T_SOURCEID   ,
                                     T_TYPE       ,
                                     T_DATE       ,
                                     T_AMOUNT     )
                             VALUES( v_RLot.t_Client,         --T_CLIENT     
                                     v_RLot.t_Contract,       --T_CONTRACT   
                                     v_RLot.t_FIID,           --T_FIID       
                                     v_CLot.t_ID,             --T_BUYID
                                     v_VS_ID,                 --T_SALEID
                                     0,                       --T_SOURCEID
                                     RSI_NPTXC.NPTXLNK_OPPOS, --T_TYPE       
                                     v_RLot.t_SaleDate,       --T_DATE    
                                     v_A                      --T_AMOUNT     
                                   );

          v_S := v_S - v_A;
        EXCEPTION
          WHEN NO_DATA_FOUND
            THEN
              BEGIN
                v_stat := 1;
              END;
        END;
      END LOOP;
      CLOSE LNK_cur;

      --иначе формируем связи по подстановкам
      v_stat := 0;

      OPEN LNK_cur FOR SELECT lnk.t_Amount - lnk.t_Short, salelot.t_ID, salelot.t_Type, lnk.t_ID
                         FROM dnptxlnk_dbt lnk, dnptxlot_dbt salelot
                        WHERE lnk.t_BuyID  = v_RLot.t_ID
                          AND lnk.t_Type   IN (RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO)
                          AND lnk.t_SaleID = salelot.t_ID
                          AND (lnk.t_Amount - lnk.t_Short) > 0
                          AND (salelot.t_BuyDate > v_RLot.t_SaleDate OR salelot.t_BuyDate = TO_DATE('01-01-0001','DD-MM-YYYY'))
                     ORDER BY salelot.t_BegSaleDate DESC,
                              salelot.t_DealDate DESC,
                              salelot.t_DealTime DESC,
                              NLSSORT(salelot.t_SortCode) DESC;

      WHILE v_S > 0 and v_stat = 0 LOOP
        FETCH LNK_cur INTO v_SPS, v_SaleLotID, v_SaleLotType, v_Lnk_ID;
        EXIT WHEN LNK_cur%NOTFOUND OR
                  LNK_cur%NOTFOUND IS NULL;

        BEGIN
          v_A   := iif( v_SPS < v_S, v_SPS, v_S );

          BEGIN
            SELECT t_ID
              INTO v_LCS_ID
              FROM dnptxlnk_dbt
             WHERE t_Type = RSI_NPTXC.NPTXLNK_SUBSTREPO
               AND t_BuyID = v_CLot.t_ID
               AND t_SaleID = v_SaleLotID
               AND t_SourceID = v_RLot.t_ID
               AND t_DATE = v_RLot.t_SaleDate;

            UPDATE dnptxlnk_dbt
               SET t_Amount = t_Amount + v_A
             WHERE t_ID = v_LCS_ID;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN

                INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                           T_CONTRACT   ,
                                           T_FIID       ,
                                           T_BUYID      ,
                                           T_SALEID     ,
                                           T_SOURCEID   ,
                                           T_TYPE       ,
                                           T_DATE       ,
                                           T_AMOUNT     )
                                   VALUES( v_RLot.t_Client,             --T_CLIENT     
                                           v_RLot.t_Contract,           --T_CONTRACT   
                                           v_RLot.t_FIID,               --T_FIID       
                                           v_CLot.t_ID,                 --T_BUYID
                                           v_SaleLotID,                 --T_SALEID
                                           v_RLot.t_ID,                 --T_SOURCEID
                                           RSI_NPTXC.NPTXLNK_SUBSTREPO, --T_TYPE       
                                           v_RLot.t_SaleDate,           --T_DATE    
                                           v_A                          --T_AMOUNT     
                                         ) RETURNING t_ID INTO v_LCS_ID;
              END;
          END;

          INSERT INTO dnptxls_dbt( t_ChildID,
                                   t_ParentID,
                                   t_Short)
                           VALUES( v_LCS_ID,
                                   v_Lnk_ID,
                                   v_A );

          v_S := v_S - v_A;

        END;

      END LOOP;
      CLOSE LNK_cur;


      SELECT * INTO v_RLot
        FROM dnptxlot_dbt
       WHERE t_ID = v_RLotID;

      LinkPart2ToBuy (v_RLot, pIIS, pContract);  

      UPDATE dnptxlot_dbt
         SET t_RetFlag  = CHR(88)
       WHERE t_ID = v_RLotID;

      UpdateTSByReverseRepo (v_RLot.t_ID, v_RLot.t_SaleDate);
    END; --ProcessCompPayOnReverseRepo

  ----Выполняет перетасовку покупок
    PROCEDURE Shuffling( pCalcDate IN DATE, pClient IN NUMBER, pFIID IN NUMBER, pIIS IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0 )  
    IS
      v_Break          BOOLEAN;
      v_SPS            NUMBER;
      v_FIID           NUMBER;
      v_LinkID         NUMBER;
      v_SaleLotID      NUMBER;
      v_BuyLotID       NUMBER;
      v_lotBegBuyDate  DATE;
      v_lotDealDate    DATE;
      v_lotDealTime    dnptxlot_dbt.t_DealTime%TYPE;
      v_lotSortCode    dnptxlot_dbt.t_SortCode%TYPE;
      v_Contract       NUMBER;
      v_BuyLotDocKind  NUMBER;
      v_BuyLotDocID    NUMBER;
      v_Buy_ID         NUMBER;
      v_Buy_Kind       NUMBER;
      v_Buy_FreeAmount NUMBER;
      v_LCS_ID         NUMBER;
      v_A              NUMBER;
      v_SL             NUMBER;

      TYPE LNKCurTyp IS REF CURSOR;
      LNK_cur LNKCurTyp;

    BEGIN

      RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Вызов Shuffling, pCalcDate = '||pCalcDate||', pClient = '||pClient);

      OPEN LNK_cur FOR SELECT (L.t_Amount - L.t_Short) SPS, B.t_FIID FIID, L.t_ID LinkID, S.t_ID, B.t_ID, 
                              B.t_BegBuyDate, B.t_DealDate, B.t_DealTime, B.t_SortCode, L.t_Contract,
                              B.t_DocKind, B.t_DocID
                         FROM dnptxlnk_dbt L, dnptxlot_dbt B, dnptxlot_dbt S
                        WHERE L.t_Type IN (RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO)
                          AND S.t_ID = L.t_SaleID
                          AND B.t_ID = L.t_BuyID
                          AND B.t_Kind = RSI_NPTXC.NPTXLOTS_BUY
                          AND S.t_Client = pClient
                          AND RSI_NPTO.CheckContrIIS(S.t_Contract)  = pIIS 
                          AND ((pContract IS NULL) OR (pContract <= 0) OR (S.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                          AND S.t_FIID = pFIID
                          AND L.t_RetFlag = CHR(0)
                          AND (L.t_Amount - L.t_Short) > 0
                          AND ((L.t_Date < pCalcDate) OR 
                               (L.t_Date = pCalcDate AND 
                                S.t_SaleDate = pCalcDate AND 
                                S.t_Type = RSI_NPTXC.NPTXDEAL_COMP
                               )
                              )
                     ORDER BY B.t_BegBuyDate ASC,
                              B.t_DealDate ASC,
                              B.t_DealTime ASC,
                              NLSSORT(B.t_SortCode) ASC,
                              B.t_ID ASC,
                              S.t_BegSaleDate DESC,
                              S.t_DealDate DESC,
                              S.t_DealTime DESC,
                              NLSSORT(S.t_SortCode) DESC,
                              S.t_ID DESC;
      LOOP

        FETCH LNK_cur INTO v_SPS, v_FIID, v_LinkID, v_SaleLotID, v_BuyLotID, 
                           v_lotBegBuyDate, v_lotDealDate, v_lotDealTime, v_lotSortCode, v_Contract,
                           v_BuyLotDocKind, v_BuyLotDocID;
        EXIT WHEN LNK_cur%NOTFOUND OR
                  LNK_cur%NOTFOUND IS NULL;

        v_Break := FALSE;

        WHILE v_SPS > 0 LOOP
          BEGIN
            SELECT *
              INTO v_Buy_ID, v_Buy_Kind, v_Buy_FreeAmount
              FROM ( SELECT /*+ INDEX_DESC (Buy DNPTXLOT_DBT_IDXB) */
                            Buy.t_ID, Buy.t_Kind, (Buy.t_Amount - Buy.t_Sale) FreeAmount
                       FROM dnptxlot_dbt Buy
                      WHERE Buy.t_FIID = v_FIID
                        AND Buy.t_Client = pClient
                        AND RSI_NPTO.CheckContrIIS(Buy.t_Contract)  = pIIS  
                        AND ((pContract IS NULL) OR (pContract <= 0) OR (Buy.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                        AND Buy.t_IsFree = CHR(88)
                        AND (Buy.t_SaleDate = TO_DATE('01.01.0001','DD.MM.YYYY') OR Buy.t_SaleDate > pCalcDate)
                        AND Buy.t_BuyDate <= pCalcDate
                        AND Buy.t_ID <> v_BuyLotID
                        AND (Buy.t_DocKind <> v_BuyLotDocKind or Buy.t_DocID <> v_BuyLotDocID)
                        AND (Buy.t_Kind <> RSI_NPTXC.NPTXLOTS_BUY OR
                             (Buy.t_BegBuyDate > v_lotBegBuyDate) OR
                             (Buy.t_BegBuyDate = v_lotBegBuyDate AND
                              (Buy.t_DealDate > v_lotDealDate OR
                               (Buy.t_DealDate = v_lotDealDate AND
                                (Buy.t_DealTime > v_lotDealTime OR
                                 (Buy.t_DealTime = v_lotDealTime AND
                                  (NLSSORT(Buy.t_SortCode) >= NLSSORT(v_lotSortCode)
                                  )
                                 )
                                )
                               )
                              )
                             )
                            )
                   ORDER BY Buy.t_OrdForSale DESC,
                            Buy.t_BegBuyDate DESC,
                            Buy.t_DealDate DESC,
                            Buy.t_DealTime DESC,
                            NLSSORT(Buy.t_SortCode) DESC,
                            Buy.t_ID DESC 
                   )
             WHERE ROWNUM = 1;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN
                v_Break := TRUE;
              END;
          END;

          EXIT WHEN v_Break;

          v_A := iif( v_SPS < v_Buy_FreeAmount, v_SPS, v_Buy_FreeAmount );
          v_SL := RSI_NPTXC.NPTXLNK_SUBSTREPO;

          BEGIN
            SELECT t_ID
              INTO v_LCS_ID
              FROM dnptxlnk_dbt
             WHERE t_Type = v_SL
               AND t_BuyID = v_Buy_ID
               AND t_SaleID = v_SaleLotID
               AND t_SourceID = v_BuyLotID
               AND t_DATE = pCalcDate;

            UPDATE dnptxlnk_dbt
               SET t_Amount = t_Amount + v_A
             WHERE t_ID = v_LCS_ID;

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              BEGIN

                INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                           T_CONTRACT   ,
                                           T_FIID       ,
                                           T_BUYID      ,
                                           T_SALEID     ,
                                           T_SOURCEID   ,
                                           T_TYPE       ,
                                           T_DATE       ,
                                           T_AMOUNT     ,
                                           T_SHORT      ,
                                           T_RETFLAG    
                                         )
                                   VALUES( pClient,                     --T_CLIENT     
                                           v_Contract,                  --T_CONTRACT   
                                           v_FIID,                      --T_FIID       
                                           v_Buy_ID,                    --T_BUYID
                                           v_SaleLotID,                 --T_SALEID
                                           v_BuyLotID,                  --T_SOURCEID
                                           v_SL,                        --T_TYPE       
                                           pCalcDate,                   --T_DATE    
                                           v_A,                         --T_AMOUNT     
                                           0,                           --T_SHORT
                                           CHR(0)                       --T_RETFLAG
                                         ) RETURNING t_ID INTO v_LCS_ID;
              END;
          END;

          INSERT INTO dnptxls_dbt( t_ChildID,
                                   t_ParentID,
                                   t_Short)
                           VALUES( v_LCS_ID,
                                   v_LinkID,
                                   v_A );
          v_SPS := v_SPS - v_A;

        END LOOP;

      END LOOP;

      CLOSE LNK_cur;
    END; --Shuffling

    -- Процедура вставки ГО
    PROCEDURE InsertGO( pBegDate IN DATE, pEndDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0 )
    IS
       v_AmountDR    NUMBER := 0;
       v_FiCodeDR    dfininstr_dbt.t_Fi_Code%TYPE;
       v_GO_CODE     DNPTXGO_DBT.t_Code%TYPE;
       v_Numerator   NUMBER := 0;
       v_Denominator NUMBER := 0;

       CURSOR cDl_COMM IS
       SELECT comm.*
         FROM DDL_COMM_DBT comm
        WHERE comm.T_DOCKIND IN (135, 139) --(Глобальная операция с ц/б, Изменение номинала ц/б)
          AND comm.T_COMMDATE  <= pEndDate
          AND comm.T_CommStatus = 2 --Закрыта
          AND NOT EXISTS (SELECT 1 FROM DNPTXGO_DBT G WHERE G.T_DOCKIND = comm.T_DOCKIND AND G.T_DOCUMENTID = comm.T_DOCUMENTID);

       CURSOR cSCDLFI(v_DocKind IN NUMBER, v_DocID IN NUMBER) IS
       SELECT *
         FROM DSCDLFI_DBT
        WHERE T_DEALKIND = v_DocKind
          AND T_DEALID = v_DocID;

    BEGIN


       FOR DL_COMM IN cDl_COMM()
       LOOP

         INSERT INTO DNPTXGO_DBT ( T_ID,
                                   T_DOCKIND,
                                   T_DOCUMENTID,
                                   T_KIND,
                                   T_CODE,
                                   T_SALEDATE,
                                   T_BUYDATE,
                                   T_FIID,
                                   T_OLDFACEVALUE,
                                   T_NEWFACEVALUE
                                 )
                          VALUES ( 0,
                                   DL_COMM.T_DOCKIND,
                                   DL_COMM.T_DOCUMENTID,
                                   (CASE WHEN DL_COMM.T_DOCKIND = 135 THEN RSI_NPTXC.NPTXLOTORIGIN_GO ELSE RSI_NPTXC.NPTXLOTORIGIN_IN END),
                                   DL_COMM.T_COMMCODE,
                                   DL_COMM.T_COMMDATE,
                                   (CASE WHEN DL_COMM.T_DOCKIND = 135 THEN DL_COMM.T_ENDDATE ELSE DL_COMM.T_COMMDATE END),
                                   DL_COMM.T_FIID,
                                   (CASE WHEN DL_COMM.T_DOCKIND = 135 THEN 0 ELSE DL_COMM.T_HIDDEN_SUM END),
                                   (CASE WHEN DL_COMM.T_DOCKIND = 135 THEN 0 ELSE DL_COMM.T_CURRENCY_SUM  END)
                                 );


         FOR SCDLFI IN cSCDLFI(DL_COMM.T_DOCKIND, DL_COMM.T_DOCUMENTID)
         LOOP
           INSERT INTO DNPTXGOFI_DBT (T_ID,
                                      T_GOID,
                                      T_NUM,
                                      T_NEWFIID,
                                      T_NUMERATOR,
                                      T_DENOMINATOR
                                     )
                              VALUES (0,
                                      (SELECT T_ID FROM DNPTXGO_DBT WHERE T_DOCKIND = DL_COMM.T_DOCKIND AND T_DOCUMENTID = DL_COMM.T_DOCUMENTID),
                                      SCDLFI.T_NUM,
                                      SCDLFI.T_NEWFIID,
                                      SCDLFI.T_NUMERATOR,
                                      SCDLFI.T_DENOMINATOR
                                     );
         END LOOP;

       END LOOP;

       --3.
       UPDATE DNPTXGO_DBT G
         SET G.T_BUYDATE = NVL((SELECT RQ2.T_FACTDATE
                                  FROM DDL_TICK_DBT TK, DDLRQ_DBT RQ2
                                 WHERE TK.T_BOFFICEKIND = G.T_DOCKIND
                                   AND TK.T_DEALID = G.T_DOCUMENTID
                                   AND TK.T_DEALDATE >= TO_DATE('01.01.2015','DD.MM.YYYY')
                                   AND TK.T_CLIENTID = pClient
                                   AND RSI_NPTO.CheckContrIIS(TK.T_CLIENTCONTRID) = pIIS
                                   AND ((pContract IS NULL) OR (pContract <= 0) OR (TK.T_CLIENTCONTRID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                   AND RQ2.T_DOCKIND = TK.T_BOFFICEKIND
                                   AND RQ2.T_DOCID = TK.T_DEALID
                                   AND RQ2.T_DEALPART = 2
                                   AND RQ2.T_TYPE = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                                   AND RQ2.T_FACTDATE >= pBegDate
                                   AND RQ2.T_FACTDATE <= pEndDate
                                   AND RQ2.T_STATE = RSI_DLRQ.DLRQ_STATE_EXEC
                                   AND tk.t_PFI = case when pFIID != -1 then pFIID else tk.t_PFI end
                                ), G.T_BUYDATE)
       WHERE G.T_DOCKIND = RSI_NPTXC.DL_CONVAVR
         AND G.T_BUYDATE = TO_DATE('01.01.0001','DD.MM.YYYY');

       --4.
       INSERT INTO DNPTXGO_DBT ( T_ID,
                                 T_DOCKIND,
                                 T_DOCUMENTID,
                                 T_KIND,
                                 T_CODE,
                                 T_SALEDATE,
                                 T_BUYDATE,
                                 T_FIID,
                                 T_OLDFACEVALUE,
                                 T_NEWFACEVALUE
                               ) 
                          SELECT 0,
                                 tk.t_BOfficeKind,
                                 tk.t_DealID,
                                 RSI_NPTXC.NPTXLOTORIGIN_GO,
                                 tk.t_DealCode,
                                 rq1.t_FactDate,
                                 rq2.t_FactDate,
                                 tk.t_PFI,
                                 0,
                                 0
                            FROM DDL_TICK_DBT tk, DDLRQ_DBT rq1, DDLRQ_DBT rq2
                           WHERE tk.t_BOfficeKind = RSI_NPTXC.DL_CONVAVR
                             AND tk.t_DealDate >= TO_DATE('01.01.2015','DD.MM.YYYY')
                             AND tk.t_ClientID = pClient
                             AND RSI_NPTO.CheckContrIIS(tk.t_ClientContrID) = pIIS
                             AND ((pContract IS NULL) OR (pContract <= 0) OR (tk.t_ClientContrID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                             AND rq1.t_DocKind = tk.t_BOfficeKind
                             AND rq1.t_DocID = tk.t_DealID
                             AND rq1.t_DealPart = 1
                             AND rq1.t_Type = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                             AND rq1.t_FactDate >= pBegDate
                             AND rq1.t_FactDate <= pEndDate
                             AND rq1.t_State = RSI_DLRQ.DLRQ_STATE_EXEC
                             AND rq2.t_DocKind = tk.t_BOfficeKind
                             AND rq2.t_DocID = tk.t_DealID
                             AND rq2.t_DealPart = 2
                             AND rq2.t_Type = RSI_DLRQ.DLRQ_TYPE_DELIVERY 
                             AND tk.t_PFI = case when pFIID != -1 then pFIID else tk.t_PFI end
                             AND NOT EXISTS (SELECT 1 FROM DNPTXGO_DBT G WHERE G.t_DocKind = tk.t_BOfficeKind AND G.t_DocumentID = tk.t_DealID);


      --5.
      FOR one_rec IN (SELECT TK.T_BOFFICEKIND, TK.T_DEALID, TK.T_DEALCODE,
                             rsb_secur.IsConvReceipt(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(TK.T_DEALTYPE, TK.T_BOFFICEKIND))) as IsConvReceipt,
                             AVR_IN.T_NUMBASEFI AS InNumBaseFI, FIN_IN.T_FI_CODE AS InFiCode, FIN_IN.T_FIID AS InFIID, 
                             AVR_OUT.T_NUMBASEFI AS OutNumBaseFI, FIN_OUT.T_FI_CODE AS OutFiCode,
                             NVL((SELECT T_ID 
                                    FROM DNPTXGO_DBT
                                   WHERE T_DOCKIND    = TK.T_BOFFICEKIND
                                     AND T_DOCUMENTID = TK.T_DEALID), 0) AS GOID
                        FROM DDL_TICK_DBT TK, DDLRQ_DBT RQ2, DFININSTR_DBT FIN_IN, DAVOIRISS_DBT AVR_IN, DFININSTR_DBT FIN_OUT, DAVOIRISS_DBT AVR_OUT
                       WHERE TK.T_BOFFICEKIND = RSI_NPTXC.DL_CONVAVR
                         AND TK.T_DEALDATE >= TO_DATE('01.01.2015','DD.MM.YYYY')
                         AND TK.T_DEALSTATUS = 20 --Закрыт
                         AND TK.T_CLIENTID = pClient
                         AND RSI_NPTO.CheckContrIIS(TK.T_CLIENTCONTRID) = pIIS
                         AND ((pContract IS NULL) OR (pContract <= 0) OR (TK.T_CLIENTCONTRID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                         AND tk.t_PFI = case when pFIID != -1 then pFIID else tk.t_PFI end
                         AND RQ2.T_DOCKIND = TK.T_BOFFICEKIND
                         AND RQ2.T_DOCID = TK.T_DEALID
                         AND RQ2.T_DEALPART = 2
                         AND RQ2.T_TYPE = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                         AND RQ2.T_FACTDATE >= pBegDate
                         AND RQ2.T_FACTDATE <= pEndDate
                         AND AVR_OUT.T_FIID = TK.T_PFI
                         AND FIN_OUT.T_FIID = AVR_OUT.T_FIID
                         AND AVR_IN.T_FIID = RQ2.T_FIID
                         AND FIN_IN.T_FIID = AVR_IN.T_FIID
                     )
      LOOP

        IF one_rec.GOID = 0 THEN
          RSI_NPTMSG.PutMsg(RSI_NPTXC.MES_WARN, 'В данных НДФЛ не найдена операция конвертации с кодом "'||one_rec.t_DealCode||'", обработана не будет');
        ELSE

           IF one_rec.IsConvReceipt <> 0 THEN
             v_AmountDR := one_rec.OutNumBaseFI;
             v_FiCodeDR := one_rec.OutFiCode;
           ELSE
             v_AmountDR := one_rec.InNumBaseFI;
             v_FiCodeDR := one_rec.InFiCode;
           END IF;

           IF v_AmountDR = 0 THEN
             SELECT T_CODE INTO v_GO_CODE FROM DNPTXGO_DBT WHERE T_ID = one_rec.GOID;
             
             RSI_NPTMSG.PutMsg(RSI_NPTXC.MES_WARN, 'Для депозитарной расписки с кодом "'||v_FiCodeDR||'" не задано количество ц/б. Операция конвертации с кодом "'||v_GO_CODE||'" обработана не будет');
           ELSE
             IF one_rec.IsConvReceipt <> 0 THEN
               v_Numerator   := v_AmountDR;
               v_Denominator := 1;
             ELSE
               v_Numerator   := 1;
               v_Denominator := v_AmountDR;
             END IF;

             INSERT INTO DNPTXGOFI_DBT (T_ID,
                                        T_GOID,
                                        T_NUM,
                                        T_NEWFIID,
                                        T_NUMERATOR,
                                        T_DENOMINATOR
                                       )
                                VALUES (0,
                                        one_rec.GOID,
                                        1,
                                        one_rec.InFIID,
                                        v_Numerator,
                                        v_Denominator
                                       );

           END IF;

        END IF;

      END LOOP;

    END;

    -- Выполняет зачисление лотов в ИН
    PROCEDURE ProcessGON(p_GO IN DNPTXGO_DBT%ROWTYPE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0)    
    IS
      v_Nnew NUMBER := 0;
      v_Nold NUMBER := 0;
      v_Fin  DFININSTR_DBT%ROWTYPE;
      v_OldAmount NUMBER := 0;
      v_NewAmount NUMBER := 0;
      v_NewLot    DNPTXLOT_DBT%ROWTYPE;
      v_CalcAmount NUMBER := 0;
      v_LotID      NUMBER;
      v_BuyDate  DATE;
      v_SaleDate DATE;
      v_BuyID  NUMBER := 0;
      v_SaleID NUMBER := 0;
      N   NUMBER := 0;

      CURSOR cL IS
             SELECT lot.*, 
                    (SELECT NVL(SUM(T.t_Amount), 0)
                       FROM DNPTXTS_DBT T
                      WHERE T.t_BuyID = lot.T_ID
                        AND T.t_Type IN (RSI_NPTXC.NPTXTS_REST, RSI_NPTXC.NPTXTS_INVST)
                        AND T.t_BegDate <= p_GO.T_SaleDate
                        AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                    ) OldAmount
               FROM DNPTXLOT_DBT lot
              WHERE lot.T_KIND = RSI_NPTXC.NPTXLOTS_BUY
                AND lot.T_CLGOID = p_GO.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS   
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND (SELECT NVL(SUM(T.t_Amount), 0)
                       FROM DNPTXTS_DBT T
                      WHERE T.t_BuyID = lot.T_ID
                        AND T.t_Type IN (RSI_NPTXC.NPTXTS_REST, RSI_NPTXC.NPTXTS_INVST)
                        AND T.t_BegDate <= p_GO.T_SaleDate
                        AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                    ) > 0;

      CURSOR cL2 IS
             SELECT lot.*, 
                    (SELECT NVL(SUM(T.t_Amount), 0)
                       FROM DNPTXTS_DBT T
                      WHERE T.t_SaleID = lot.T_ID
                        AND T.t_Type = RSI_NPTXC.NPTXTS_SHPOS
                        AND T.t_BegDate <= p_GO.T_SaleDate
                        AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                    ) OldAmount
               FROM DNPTXLOT_DBT lot
              WHERE lot.T_KIND = RSI_NPTXC.NPTXLOTS_SALE
                AND lot.T_CLGOID = p_GO.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS  
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND (SELECT NVL(SUM(T.t_Amount), 0)
                       FROM DNPTXTS_DBT T
                      WHERE T.t_SaleID = lot.T_ID
                        AND T.t_Type = RSI_NPTXC.NPTXTS_SHPOS
                        AND T.t_BegDate <= p_GO.T_SaleDate
                        AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                    ) > 0;

      CURSOR cL3 IS
             SELECT lot.*
               FROM DNPTXLOT_DBT lot
              WHERE lot.T_KIND IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_BACKREPO,
                                   RSI_NPTXC.NPTXLOTS_LOANPUT, RSI_NPTXC.NPTXLOTS_LOANGET)
                AND lot.T_CLGOID = p_GO.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)));

      CURSOR cRest1 IS
             SELECT lot.t_Client, lot.t_Contract, T.t_BUYID, T.t_SALEID, T.t_Amount
               FROM DNPTXLOT_DBT lot, DNPTXTS_DBT T
              WHERE lot.T_KIND = RSI_NPTXC.NPTXLOTS_SALE
                AND lot.T_CLGOID = p_GO.T_ID
                AND T.t_SaleID = lot.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS 
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND T.t_Type = RSI_NPTXC.NPTXTS_SHPOS
                AND T.t_BegDate <= p_GO.T_SaleDate
                AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                AND T.t_Amount > 0;

      CURSOR cRest2 IS
             SELECT lot.t_Client, lot.t_Contract, T.t_BUYID, T.t_SALEID, T.t_Amount
               FROM DNPTXLOT_DBT lot, DNPTXTS_DBT T
              WHERE lot.T_KIND = RSI_NPTXC.NPTXLOTS_BUY
                AND lot.T_CLGOID = p_GO.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS 
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND T.t_BuyID = lot.T_ID
                AND T.t_Type = RSI_NPTXC.NPTXTS_INVST
                AND T.t_BegDate <= p_GO.T_SaleDate
                AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                AND T.t_Amount > 0;

    BEGIN

      SELECT * INTO v_Fin
        FROM DFININSTR_DBT
       WHERE t_FIID = p_GO.T_FIID;

      FOR L IN cL LOOP
        v_OldAmount := L.OldAmount;
        v_Nold      := v_Nold + v_OldAmount;
        v_NewAmount := v_OldAmount * p_GO.T_OLDFACEVALUE / p_GO.T_NEWFACEVALUE;
        v_Nnew      := v_Nnew + v_NewAmount;

        INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                   T_DOCID          ,
                                   T_RQID           ,
                                   T_DEALDATE       ,
                                   T_DEALTIME       ,
                                   T_DEALCODE       ,
                                   T_DEALCODETS     ,
                                   T_CLIENT         ,
                                   T_CONTRACT       ,
                                   T_FIID           ,
                                   T_KIND           ,
                                   T_TYPE           ,
                                   T_BUYID          ,
                                   T_REALID         ,
                                   T_AMOUNT         ,
                                   T_SALE           ,
                                   T_COMPAMOUNT     ,
                                   T_PRICE          ,
                                   T_PRICEFIID      ,
                                   T_TOTALCOST      ,
                                   T_NKD            ,
                                   T_BUYDATE        ,
                                   T_SALEDATE       ,
                                   T_SORTCODE       ,
                                   T_RETFLAG        ,
                                   T_ISFREE         ,
                                   T_BEGLOTID       ,
                                   T_CHILDID        ,
                                   T_BEGBUYDATE     ,
                                   T_BEGSALEDATE    ,
                                   T_INACC          ,
                                   T_BLOCKED        ,
                                   T_ORIGIN         ,
                                   T_GOID           ,
                                   T_CLGOID         ,
                                   T_OLDDATE        ,
                                   T_ORDFORSALE     ,
                                   T_ORDFORREPO     ,
                                   T_NOTCOUNTEDONIIS
                                 )
                           VALUES( L.t_DocKind,                                --T_DOCKIND    
                                   L.t_DocID,                                  --T_DOCID      
                                   L.t_RQID,                                   --T_RQID  
                                   L.t_DealDate,                               --T_DEALDATE
                                   L.t_DealTime,                               --T_DEALTIME
                                   L.t_DealCode,                               --T_DEALCODE
                                   L.t_DealCodeTS,                             --T_DEALCODETS
                                   L.t_Client,                                 --T_CLIENT     
                                   L.t_Contract,                               --T_CONTRACT   
                                   p_GO.T_FIID,                                --T_FIID       
                                   RSI_NPTXC.NPTXLOTS_BUY,                     --T_KIND       
                                   RSI_NPTXC.NPTXDEAL_REAL,                    --T_TYPE       
                                   0,                                          --T_BUYID
                                   L.t_ID,                                     --T_REALID
                                   v_NewAmount,                                --T_AMOUNT     
                                   0,                                          --T_SALE     
                                   0,                                          --T_COMPAMOUNT     
                                   L.t_Price*v_OldAmount/v_NewAmount,          --T_PRICE    
                                   L.t_PriceFIID,                              --T_PRICEFIID
                                   L.t_TotalCost*v_OldAmount/L.t_Amount,       --T_TOTALCOST
                                   L.t_NKD*v_OldAmount/L.t_Amount,             --T_NKD      
                                   L.t_BuyDate,                                --T_BUYDATE    
                                   TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_SALEDATE   
                                   L.t_SortCode,                               --T_SORTCODE
                                   CHR(0),                                     --T_RETFLAG
                                   CHR(0),                                     --T_ISFREE
                                   L.t_BegLotID,                               --T_BEGLOTID
                                   0,                                          --T_CHILDID 
                                   L.t_BegBuyDate,                             --T_BEGBUYDATE    
                                   TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BEGSALEDATE   
                                   CHR(88),                                    --T_INACC
                                   CHR(0),                                     --T_BLOCKED
                                   RSI_NPTXC.NPTXLOTORIGIN_IN,                 --T_ORIGIN
                                   p_GO.T_ID,                                  --T_GOID  
                                   0,                                          --T_CLGOID 
                                   TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_OLDDATE   
                                   0,                                          --T_ORDFORSALE
                                   0,                                          --T_ORDFORREPO
                                   RSI_NPTX.DetermineIISCountingStatus(L.t_DocKind, L.t_DocID, L.t_Contract) --T_NOTCOUNTEDONIIS
                                 );

        IF N = 0 THEN
          RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Произведено зачисление ц/б '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                                            ' в рамках обработки корпоративных действий по операции № '||p_GO.T_CODE||' на дату '|| TO_CHAR(p_GO.T_BUYDATE,'DD.MM.YYYY') );
        END IF;
        N := N + 1;

        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'По сделке № '||L.T_DEALCODETS||' '||v_NewAmount||' бумаг' || ' стоимостью ' || L.t_TotalCost*v_OldAmount/L.t_Amount );
      END LOOP;

      FOR L IN cL2 LOOP
        v_OldAmount := L.OldAmount;
        v_NewAmount := v_OldAmount * p_GO.T_OLDFACEVALUE / p_GO.T_NEWFACEVALUE;
        v_Nold      := v_Nold + v_OldAmount;
        v_Nnew      := v_Nnew + v_NewAmount;

        INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                   T_DOCID          ,
                                   T_RQID           ,
                                   T_DEALDATE       ,
                                   T_DEALTIME       ,
                                   T_DEALCODE       ,
                                   T_DEALCODETS     ,
                                   T_CLIENT         ,
                                   T_CONTRACT       ,
                                   T_FIID           ,
                                   T_KIND           ,
                                   T_TYPE           ,
                                   T_BUYID          ,
                                   T_REALID         ,
                                   T_AMOUNT         ,
                                   T_SALE           ,
                                   T_COMPAMOUNT     ,
                                   T_PRICE          ,
                                   T_PRICEFIID      ,
                                   T_TOTALCOST      ,
                                   T_NKD            ,
                                   T_BUYDATE        ,
                                   T_SALEDATE       ,
                                   T_SORTCODE       ,
                                   T_RETFLAG        ,
                                   T_ISFREE         ,
                                   T_BEGLOTID       ,
                                   T_CHILDID        ,
                                   T_BEGBUYDATE     ,
                                   T_BEGSALEDATE    ,
                                   T_INACC          ,
                                   T_BLOCKED        ,
                                   T_ORIGIN         ,
                                   T_GOID           ,
                                   T_CLGOID         ,
                                   T_OLDDATE        ,
                                   T_ORDFORSALE     ,
                                   T_ORDFORREPO     ,
                                   T_NOTCOUNTEDONIIS
                                 )
                           VALUES( L.t_DocKind,                                --T_DOCKIND    
                                   L.t_DocID,                                  --T_DOCID      
                                   L.t_RQID,                                   --T_RQID  
                                   L.t_DealDate,                               --T_DEALDATE
                                   L.t_DealTime,                               --T_DEALTIME
                                   L.t_DealCode,                               --T_DEALCODE
                                   L.t_DealCodeTS,                             --T_DEALCODETS
                                   L.t_Client,                                 --T_CLIENT     
                                   L.t_Contract,                               --T_CONTRACT   
                                   p_GO.T_FIID,                                --T_FIID       
                                   RSI_NPTXC.NPTXLOTS_SALE,                    --T_KIND       
                                   RSI_NPTXC.NPTXDEAL_REAL,                    --T_TYPE       
                                   0,                                          --T_BUYID
                                   L.t_ID,                                     --T_REALID
                                   v_NewAmount,                                --T_AMOUNT     
                                   0,                                          --T_SALE     
                                   0,                                          --T_COMPAMOUNT     
                                   L.t_Price*v_OldAmount/v_NewAmount,          --T_PRICE    
                                   L.t_PriceFIID,                              --T_PRICEFIID
                                   L.t_TotalCost*v_OldAmount/L.t_Amount,       --T_TOTALCOST
                                   L.t_NKD*v_OldAmount/L.t_Amount,             --T_NKD      
                                   TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BUYDATE    
                                   p_GO.t_BuyDate,                             --T_SALEDATE   
                                   L.t_SortCode,                               --T_SORTCODE
                                   CHR(0),                                     --T_RETFLAG
                                   CHR(0),                                     --T_ISFREE
                                   L.t_BegLotID,                               --T_BEGLOTID
                                   0,                                          --T_CHILDID 
                                   TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BEGBUYDATE    
                                   L.t_BegSaleDate,                            --T_BEGSALEDATE   
                                   CHR(88),                                    --T_INACC
                                   CHR(0),                                     --T_BLOCKED
                                   RSI_NPTXC.NPTXLOTORIGIN_IN,                 --T_ORIGIN
                                   p_GO.T_ID,                                  --T_GOID  
                                   0,                                          --T_CLGOID 
                                   TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_OLDDATE   
                                   0,                                          --T_ORDFORSALE
                                   0,                                          --T_ORDFORREPO
                                   RSI_NPTX.DetermineIISCountingStatus(L.t_DocKind, L.t_DocID, L.t_Contract) --T_NOTCOUNTEDONIIS
                                 );

        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'По сделке № '||L.T_DEALCODETS||' '||v_NewAmount||' бумаг' || ' стоимостью ' || L.t_TotalCost*v_OldAmount/L.t_Amount   );
      END LOOP;


      FOR L IN cL3 LOOP
        IF(L.T_OLDDATE < p_GO.T_BUYDATE AND L.T_OLDDATE <> TO_DATE('01.01.0001','DD.MM.YYYY') AND L.T_OLDDATE IS NOT NULL) THEN
           RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Дата второй части '||L.T_OLDDATE||' операции Репо '||L.T_DEALCODETS||' меньше даты зачисления '||p_GO.T_BUYDATE||' операции конвертации '||p_GO.T_CODE );
        ELSE
           v_OldAmount := L.t_Amount;
           v_NewAmount := v_OldAmount * p_GO.T_OLDFACEVALUE / p_GO.T_NEWFACEVALUE;
           v_Nold      := v_Nold + v_OldAmount;
           v_Nnew      := v_Nnew + v_NewAmount;

           IF (L.t_Kind IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)) THEN
              v_BuyDate  := L.T_OLDDATE;
              v_SaleDate := p_GO.T_BUYDATE;
           ELSE
              v_BuyDate  := p_GO.T_BUYDATE;
              v_SaleDate := L.T_OLDDATE;
           END IF;

           INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                      T_DOCID          ,
                                      T_RQID           ,
                                      T_DEALDATE       ,
                                      T_DEALTIME       ,
                                      T_DEALCODE       ,
                                      T_DEALCODETS     ,
                                      T_CLIENT         ,
                                      T_CONTRACT       ,
                                      T_FIID           ,
                                      T_KIND           ,
                                      T_TYPE           ,
                                      T_BUYID          ,
                                      T_REALID         ,
                                      T_AMOUNT         ,
                                      T_SALE           ,
                                      T_COMPAMOUNT     ,
                                      T_PRICE          ,
                                      T_PRICEFIID      ,
                                      T_TOTALCOST      ,
                                      T_NKD            ,
                                      T_BUYDATE        ,
                                      T_SALEDATE       ,
                                      T_SORTCODE       ,
                                      T_RETFLAG        ,
                                      T_ISFREE         ,
                                      T_BEGLOTID       ,
                                      T_CHILDID        ,
                                      T_BEGBUYDATE     ,
                                      T_BEGSALEDATE    ,
                                      T_INACC          ,
                                      T_BLOCKED        ,
                                      T_ORIGIN         ,
                                      T_GOID           ,
                                      T_CLGOID         ,
                                      T_OLDDATE        ,
                                      T_ORDFORSALE     ,
                                      T_ORDFORREPO     ,
                                      T_NOTCOUNTEDONIIS
                                    )
                              VALUES( L.t_DocKind,                                --T_DOCKIND    
                                      L.t_DocID,                                  --T_DOCID      
                                      L.t_RQID,                                   --T_RQID  
                                      L.t_DealDate,                               --T_DEALDATE
                                      L.t_DealTime,                               --T_DEALTIME
                                      L.t_DealCode,                               --T_DEALCODE
                                      L.t_DealCodeTS,                             --T_DEALCODETS
                                      L.t_Client,                                 --T_CLIENT     
                                      L.t_Contract,                               --T_CONTRACT   
                                      p_GO.T_FIID,                                --T_FIID       
                                      L.t_Kind,                                   --T_KIND       
                                      RSI_NPTXC.NPTXDEAL_REAL,                    --T_TYPE       
                                      0,                                          --T_BUYID
                                      L.t_ID,                                     --T_REALID
                                      v_NewAmount,                                --T_AMOUNT     
                                      0,                                          --T_SALE     
                                      0,                                          --T_COMPAMOUNT     
                                      L.t_Price*v_OldAmount/v_NewAmount,          --T_PRICE    
                                      L.t_PriceFIID,                              --T_PRICEFIID
                                      L.t_TotalCost*v_OldAmount/L.t_Amount,       --T_TOTALCOST
                                      L.t_NKD*v_OldAmount/L.t_Amount,             --T_NKD      
                                      v_BuyDate,                                  --T_BUYDATE    
                                      v_SaleDate,                                 --T_SALEDATE   
                                      L.t_SortCode,                               --T_SORTCODE
                                      CHR(0),                                     --T_RETFLAG
                                      CHR(0),                                     --T_ISFREE
                                      L.t_BegLotID,                               --T_BEGLOTID
                                      0,                                          --T_CHILDID 
                                      L.t_BegBuyDate,                             --T_BEGBUYDATE    
                                      L.t_BegSaleDate,                            --T_BEGSALEDATE   
                                      CHR(88),                                    --T_INACC
                                      L.t_Blocked,                                --T_BLOCKED
                                      RSI_NPTXC.NPTXLOTORIGIN_IN,                 --T_ORIGIN
                                      p_GO.T_ID,                                  --T_GOID  
                                      0,                                          --T_CLGOID 
                                      TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_OLDDATE   
                                      0,                                          --T_ORDFORSALE
                                      0,                                          --T_ORDFORREPO
                                      RSI_NPTX.DetermineIISCountingStatus(L.t_DocKind, L.t_DocID, L.t_Contract) --T_NOTCOUNTEDONIIS
                                    );

           RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'По сделке № '||L.T_DEALCODETS||' '||v_NewAmount||' бумаг' || ' стоимостью ' || L.t_TotalCost*v_OldAmount/L.t_Amount   );
        END IF;
      END LOOP;

      v_CalcAmount := round(v_Nold * p_GO.T_OLDFACEVALUE / p_GO.T_NEWFACEVALUE, v_Fin.T_SUMPRECISION);

      IF v_Nnew <> v_CalcAmount THEN
         RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Общее количество ц/б по лотам выпуска '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                                               ' в системе '||v_Nnew||' штук, расчетное количество по операции '||v_CalcAmount||' штук, необходимо провести коррекцию количества на налоговых лотах');
         FOR Rest1 IN cRest1 LOOP
           
            BEGIN
               SELECT t_ID 
                 INTO v_BuyID
                 FROM dnptxlot_dbt
                WHERE t_GOID = p_GO.T_ID
                  AND t_RealID = Rest1.t_BUYID
                  AND t_Kind IN (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET)
                  AND ROWNUM = 1;
            EXCEPTION
              WHEN NO_DATA_FOUND
                THEN
                  BEGIN
                    v_BuyID := 0;
                  END;
            END;

            BEGIN
               SELECT t_ID 
                 INTO v_SaleID
                 FROM dnptxlot_dbt
                WHERE t_GOID = p_GO.T_ID
                  AND t_RealID = Rest1.t_SALEID
                  AND t_Kind = RSI_NPTXC.NPTXLOTS_SALE
                  AND ROWNUM = 1;

            EXCEPTION
              WHEN NO_DATA_FOUND
                THEN
                  BEGIN
                    v_SaleID := 0;
                  END;
            END;

            INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                       T_CONTRACT   ,
                                       T_FIID       ,
                                       T_BUYID      ,
                                       T_SALEID     ,
                                       T_SOURCEID   ,
                                       T_TYPE       ,
                                       T_DATE       ,
                                       T_AMOUNT     ,
                                       T_SHORT      ,
                                       T_RETFLAG    
                                     )
                               VALUES( Rest1.t_Client,                --T_CLIENT     
                                       Rest1.t_Contract,              --T_CONTRACT   
                                       p_GO.t_FIID,                   --T_FIID       
                                       v_BuyID,                       --T_BUYID
                                       v_SaleID,                      --T_SALEID
                                       0,                             --T_SOURCEID
                                       RSI_NPTXC.NPTXLNK_OPPOS,       --T_TYPE       
                                       p_GO.t_BuyDate,                --T_DATE    
                                       Rest1.t_Amount * p_GO.T_OLDFACEVALUE / p_GO.T_NEWFACEVALUE, --T_AMOUNT     
                                       0,                             --T_SHORT
                                       CHR(0)                         --T_RETFLAG
                                     );

         END LOOP;

         FOR Rest2 IN cRest2 LOOP
           
            BEGIN
               SELECT t_ID 
                 INTO v_BuyID
                 FROM dnptxlot_dbt
                WHERE t_GOID = p_GO.T_ID
                  AND t_RealID = Rest2.t_BUYID
                  AND t_Kind = RSI_NPTXC.NPTXLOTS_BUY
                  AND ROWNUM = 1;
            EXCEPTION
              WHEN NO_DATA_FOUND
                THEN
                  BEGIN
                    v_BuyID := 0;
                  END;
            END;

            BEGIN
               SELECT t_ID 
                 INTO v_SaleID
                 FROM dnptxlot_dbt
                WHERE t_GOID = p_GO.T_ID
                  AND t_RealID = Rest2.t_SALEID
                  AND t_Kind IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)
                  AND ROWNUM = 1;
            EXCEPTION
              WHEN NO_DATA_FOUND
                THEN
                  BEGIN
                    v_SaleID := 0;
                  END;
            END;

            INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                       T_CONTRACT   ,
                                       T_FIID       ,
                                       T_BUYID      ,
                                       T_SALEID     ,
                                       T_SOURCEID   ,
                                       T_TYPE       ,
                                       T_DATE       ,
                                       T_AMOUNT     ,
                                       T_SHORT      ,
                                       T_RETFLAG    
                                     )
                               VALUES( Rest2.t_Client,                --T_CLIENT     
                                       Rest2.t_Contract,              --T_CONTRACT   
                                       p_GO.t_FIID,                   --T_FIID       
                                       v_BuyID,                       --T_BUYID
                                       v_SaleID,                      --T_SALEID
                                       0,                             --T_SOURCEID
                                       RSI_NPTXC.NPTXLNK_REPO,        --T_TYPE       
                                       p_GO.t_BuyDate,                --T_DATE    
                                       Rest2.t_Amount * p_GO.T_OLDFACEVALUE / p_GO.T_NEWFACEVALUE, --T_AMOUNT     
                                       0,                             --T_SHORT
                                       CHR(0)                         --T_RETFLAG
                                     );
         END LOOP;
      END IF;
    END; --ProcessGON

    --Выполняет списание лотов в ГО и ИН
    PROCEDURE ProcessGO(p_GO IN DNPTXGO_DBT%ROWTYPE, pClient IN NUMBER, v_IIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0 )  
    IS
      v_SPS        NUMBER := 0;
      v_BUY_ID     NUMBER := 0;
      v_LCS_ID     NUMBER := 0;
      v_Amount     NUMBER := 0;
      v_SALE_ID    NUMBER := 0;
      v_Fin        DFININSTR_DBT%ROWTYPE;
      v_ExistSale   BOOLEAN := False;

      CURSOR cRepo IS
             SELECT *
               FROM DNPTXLOT_DBT Repo
              WHERE Repo.T_KIND IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)
                AND Repo.T_FIID = p_GO.T_FIID
                AND Repo.T_SALEDATE <= p_GO.T_SALEDATE
                AND (Repo.T_BUYDATE >= p_GO.T_SALEDATE OR Repo.T_BUYDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR Repo.T_BUYDATE IS NULL)
                AND Repo.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(Repo.t_Contract)  = v_IIS
                AND ((pContract IS NULL) OR (pContract <= 0) OR (Repo.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND Repo.t_FIID = case when pFIID != -1 then pFIID else Repo.t_FIID end
                AND Repo.T_INACC = CHR(88)
                AND Repo.T_RETFLAG <> CHR(88);

      CURSOR cLnk(RepoID IN NUMBER) IS
             SELECT *
               FROM DNPTXLNK_DBT Lnk
              WHERE Lnk.t_Type IN (RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO)
                AND Lnk.t_SaleID = RepoID;

      CURSOR cPart2Lot IS
             SELECT *
               FROM DNPTXLOT_DBT Repo
              WHERE Repo.T_KIND IN (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET)
                AND Repo.T_FIID = p_GO.T_FIID
                AND Repo.T_BUYDATE <= p_GO.T_SALEDATE
                AND (Repo.T_SALEDATE >= p_GO.T_SALEDATE OR Repo.T_SALEDATE = TO_DATE('01.01.0001','DD.MM.YYYY') OR Repo.T_SALEDATE IS NULL)
                AND Repo.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(Repo.t_Contract)  = v_IIS  
                AND ((pContract IS NULL) OR (pContract <= 0) OR (Repo.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND Repo.t_FIID = case when pFIID != -1 then pFIID else Repo.t_FIID end
                AND Repo.T_INACC = CHR(88)
                AND Repo.T_RETFLAG <> CHR(88);

      CURSOR cOSPLNK(Part2LotID IN NUMBER) IS
             SELECT lnk.t_Amount, lnk.t_Short, lnk.t_ID t_LnkID, lnk.t_SaleID,
                    salelot.t_DocKind, salelot.t_DocID, salelot.t_RQID, salelot.t_DealDate,   
                    salelot.t_DealTime, salelot.t_DealCode, salelot.t_DealCodeTS
               FROM dnptxlnk_dbt lnk, dnptxlot_dbt salelot
              WHERE lnk.t_BuyID = Part2LotID
                AND lnk.t_Type = RSI_NPTXC.NPTXLNK_OPPOS
                AND salelot.t_ID = lnk.t_SaleID
                AND (lnk.t_Amount - lnk.t_Short) > 0;

      CURSOR cBuyLot IS
             SELECT *
               FROM DNPTXLOT_DBT Lot
              WHERE Lot.T_KIND = RSI_NPTXC.NPTXLOTS_BUY
                AND Lot.T_FIID = p_GO.T_FIID
                AND Lot.T_BUYDATE <= p_GO.T_SALEDATE
                AND Lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(Lot.t_Contract)  = v_IIS   
                AND ((pContract IS NULL) OR (pContract <= 0) OR (Lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND Lot.t_FIID = case when pFIID != -1 then pFIID else Lot.t_FIID end
                AND Lot.T_ISFREE = CHR(88)
                AND 1 = (CASE WHEN Lot.t_DocKind = RSI_NPTXC.DL_AVRWRT AND Lot.t_DealDate > p_GO.T_SALEDATE THEN 0 ELSE 1 END);

    BEGIN

      FOR Repo IN cRepo LOOP
         IF (Repo.T_CHILDID <> 0) THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Не должно быть связанных докомпенсационных лотов в дату списания конвертации' );
         ELSE
            FOR Lnk IN cLnk(Repo.t_ID) LOOP
               UPDATE dnptxlnk_dbt
                  SET t_RetFlag = CHR(88)
                WHERE t_ID = Lnk.t_ID;

               UPDATE dnptxlot_dbt lot
                  SET lot.t_CLGOID = p_GO.t_ID
                WHERE lot.t_ID = Lnk.t_SaleID
                  AND lot.t_Kind = RSI_NPTXC.NPTXLOTS_BUY;
            END LOOP;

            UPDATE dnptxlot_dbt
               SET t_OldDate = t_BuyDate,
                   t_BuyDate = p_GO.t_SaleDate,
                   t_RetFlag = CHR(88),
                   t_ClGOID  = p_GO.t_ID
             WHERE t_ID = Repo.t_ID;
         END IF;
      END LOOP;

      FOR Part2Lot IN cPart2Lot LOOP
         IF (Part2Lot.T_CHILDID <> 0) THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Не должно быть связанных докомпенсационных лотов в дату списания конвертации' );
         ELSE
            FOR Lnk IN cOSPLNK(Part2Lot.t_ID) LOOP
               v_SPS := Lnk.t_Amount - Lnk.t_Short;

               INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                          T_DOCID          ,
                                          T_RQID           ,
                                          T_DEALDATE       ,
                                          T_DEALTIME       ,
                                          T_DEALCODE       ,
                                          T_DEALCODETS     ,
                                          T_CLIENT         ,
                                          T_CONTRACT       ,
                                          T_FIID           ,
                                          T_KIND           ,
                                          T_TYPE           ,
                                          T_BUYID          ,
                                          T_REALID         ,
                                          T_AMOUNT         ,
                                          T_SALE           ,
                                          T_COMPAMOUNT     ,
                                          T_PRICE          ,
                                          T_PRICEFIID      ,
                                          T_TOTALCOST      ,
                                          T_NKD            ,
                                          T_BUYDATE        ,
                                          T_SALEDATE       ,
                                          T_SORTCODE       ,
                                          T_RETFLAG        ,
                                          T_ISFREE         ,
                                          T_BEGLOTID       ,
                                          T_CHILDID        ,
                                          T_BEGBUYDATE     ,
                                          T_BEGSALEDATE    ,
                                          T_INACC          ,
                                          T_BLOCKED        ,
                                          T_ORIGIN         ,
                                          T_GOID           ,
                                          T_CLGOID         ,
                                          T_OLDDATE        ,
                                          T_ORDFORSALE     ,
                                          T_ORDFORREPO     ,
                                          T_NOTCOUNTEDONIIS
                                        )
                                  VALUES( Lnk.t_DocKind,                              --T_DOCKIND    
                                          Lnk.t_DocID,                                --T_DOCID      
                                          Lnk.t_RQID,                                 --T_RQID  
                                          Lnk.t_DealDate,                             --T_DEALDATE
                                          Lnk.t_DealTime,                             --T_DEALTIME
                                          Lnk.t_DealCode,                             --T_DEALCODE
                                          Lnk.t_DealCodeTS,                           --T_DEALCODETS
                                          Part2Lot.t_Client,                          --T_CLIENT     
                                          Part2Lot.t_Contract,                        --T_CONTRACT   
                                          p_GO.T_FIID,                                --T_FIID       
                                          RSI_NPTXC.NPTXLOTS_BUY,                     --T_KIND       
                                          RSI_NPTXC.NPTXDEAL_REAL,                    --T_TYPE       
                                          0,                                          --T_BUYID
                                          Lnk.t_SaleID,                               --T_REALID
                                          v_SPS,                                      --T_AMOUNT     
                                          0,                                          --T_SALE     
                                          0,                                          --T_COMPAMOUNT     
                                          0,                                          --T_PRICE    
                                          0,                                          --T_PRICEFIID
                                          0,                                          --T_TOTALCOST
                                          0,                                          --T_NKD      
                                          p_GO.t_SaleDate,                            --T_BUYDATE    
                                          TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_SALEDATE   
                                          CHR(1),                                     --T_SORTCODE
                                          CHR(0),                                     --T_RETFLAG
                                          CHR(0),                                     --T_ISFREE
                                          0,                                          --T_BEGLOTID
                                          0,                                          --T_CHILDID 
                                          p_GO.t_SaleDate,                            --T_BEGBUYDATE    
                                          TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BEGSALEDATE   
                                          CHR(88),                                    --T_INACC
                                          CHR(0),                                     --T_BLOCKED
                                          p_GO.T_KIND,                                --T_ORIGIN
                                          p_GO.T_ID,                                  --T_GOID  
                                          0,                                          --T_CLGOID 
                                          TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_OLDDATE   
                                          0,                                          --T_ORDFORSALE
                                          0,                                          --T_ORDFORREPO
                                          RSI_NPTX.DetermineIISCountingStatus(Lnk.t_DocKind, Lnk.t_DocID, Part2Lot.t_Contract) --T_NOTCOUNTEDONIIS
                                        ) RETURNING t_ID INTO v_BUY_ID;

               INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                          T_CONTRACT   ,
                                          T_FIID       ,
                                          T_BUYID      ,
                                          T_SALEID     ,
                                          T_SOURCEID   ,
                                          T_TYPE       ,
                                          T_DATE       ,
                                          T_AMOUNT     ,
                                          T_SHORT      ,
                                          T_RETFLAG    
                                        )
                                  VALUES( Part2Lot.t_Client,             --T_CLIENT     
                                          Part2Lot.t_Contract,           --T_CONTRACT   
                                          p_GO.t_FIID,                   --T_FIID       
                                          v_BUY_ID,                      --T_BUYID
                                          Lnk.t_SaleID,                  --T_SALEID
                                          Part2Lot.t_ID,                 --T_SOURCEID
                                          RSI_NPTXC.NPTXLNK_CLPOS,       --T_TYPE       
                                          p_GO.t_SaleDate,               --T_DATE    
                                          v_SPS,                         --T_AMOUNT     
                                          0,                             --T_SHORT
                                          CHR(0)                         --T_RETFLAG
                                        ) RETURNING t_ID INTO v_LCS_ID;

               INSERT INTO dnptxls_dbt( t_ChildID,
                                        t_ParentID,
                                        t_Short)
                                VALUES( v_LCS_ID,
                                        Lnk.t_LnkID,
                                        v_SPS );

               UPDATE dnptxlot_dbt
                  SET t_ClGOID  = p_GO.t_ID
                WHERE t_ID = Lnk.t_SaleID
                  AND t_ClGOID <> p_GO.t_ID;
            END LOOP;

            UPDATE dnptxlot_dbt
               SET t_OldDate = t_SaleDate,
                   t_SaleDate = p_GO.t_SaleDate,
                   t_RetFlag = CHR(88),
                   t_ClGOID  = p_GO.t_ID
             WHERE t_ID = Part2Lot.t_ID;

            UpdateTSByReverseRepo (Part2Lot.t_ID, p_GO.t_SaleDate);

         END IF;
      END LOOP;

      FOR BuyLot IN cBuyLot LOOP
         IF IsVirtual(BuyLot.t_Type) = 1 THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Есть виртуальная покупка '||BuyLot.t_DealCodeTS||' на дату конвертации '||p_GO.T_SALEDATE );
         ELSE
            v_Amount := BuyLot.t_Amount - BuyLot.t_Sale;

            INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                       T_DOCID          ,
                                       T_RQID           ,
                                       T_DEALDATE       ,
                                       T_DEALTIME       ,
                                       T_DEALCODE       ,
                                       T_DEALCODETS     ,
                                       T_CLIENT         ,
                                       T_CONTRACT       ,
                                       T_FIID           ,
                                       T_KIND           ,
                                       T_TYPE           ,
                                       T_BUYID          ,
                                       T_REALID         ,
                                       T_AMOUNT         ,
                                       T_SALE           ,
                                       T_COMPAMOUNT     ,
                                       T_PRICE          ,
                                       T_PRICEFIID      ,
                                       T_TOTALCOST      ,
                                       T_NKD            ,
                                       T_BUYDATE        ,
                                       T_SALEDATE       ,
                                       T_SORTCODE       ,
                                       T_RETFLAG        ,
                                       T_ISFREE         ,
                                       T_BEGLOTID       ,
                                       T_CHILDID        ,
                                       T_BEGBUYDATE     ,
                                       T_BEGSALEDATE    ,
                                       T_INACC          ,
                                       T_BLOCKED        ,
                                       T_ORIGIN         ,
                                       T_GOID           ,
                                       T_CLGOID         ,
                                       T_OLDDATE        ,
                                       T_ORDFORSALE     ,
                                       T_ORDFORREPO     ,
                                       T_NOTCOUNTEDONIIS
                                     )
                               VALUES( BuyLot.t_DocKind,                           --T_DOCKIND    
                                       BuyLot.t_DocID,                             --T_DOCID      
                                       BuyLot.t_RQID,                              --T_RQID  
                                       BuyLot.t_DealDate,                          --T_DEALDATE
                                       BuyLot.t_DealTime,                          --T_DEALTIME
                                       BuyLot.t_DealCode,                          --T_DEALCODE
                                       BuyLot.t_DealCodeTS,                        --T_DEALCODETS
                                       BuyLot.t_Client,                            --T_CLIENT     
                                       BuyLot.t_Contract,                          --T_CONTRACT   
                                       p_GO.T_FIID,                                --T_FIID       
                                       RSI_NPTXC.NPTXLOTS_SALE,                    --T_KIND       
                                       RSI_NPTXC.NPTXDEAL_REAL,                    --T_TYPE       
                                       0,                                          --T_BUYID
                                       BuyLot.t_ID,                                --T_REALID
                                       v_Amount,                                   --T_AMOUNT     
                                       0,                                          --T_SALE     
                                       0,                                          --T_COMPAMOUNT     
                                       0,                                          --T_PRICE    
                                       0,                                          --T_PRICEFIID
                                       0,                                          --T_TOTALCOST
                                       0,                                          --T_NKD      
                                       TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BUYDATE    
                                       p_GO.t_SaleDate,                            --T_SALEDATE   
                                       CHR(1),                                     --T_SORTCODE
                                       CHR(0),                                     --T_RETFLAG
                                       CHR(0),                                     --T_ISFREE
                                       0,                                          --T_BEGLOTID
                                       0,                                          --T_CHILDID 
                                       TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BEGBUYDATE    
                                       p_GO.t_SaleDate,                            --T_BEGSALEDATE   
                                       CHR(88),                                    --T_INACC
                                       CHR(0),                                     --T_BLOCKED
                                       p_GO.T_KIND,                                --T_ORIGIN
                                       p_GO.T_ID,                                  --T_GOID  
                                       0,                                          --T_CLGOID 
                                       TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_OLDDATE   
                                       0,                                          --T_ORDFORSALE
                                       0,                                          --T_ORDFORREPO
                                       RSI_NPTX.DetermineIISCountingStatus(BuyLot.t_DocKind, BuyLot.t_DocID, BuyLot.t_Contract) --T_NOTCOUNTEDONIIS
                                     ) RETURNING t_ID INTO v_SALE_ID;

            INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                       T_CONTRACT   ,
                                       T_FIID       ,
                                       T_BUYID      ,
                                       T_SALEID     ,
                                       T_SOURCEID   ,
                                       T_TYPE       ,
                                       T_DATE       ,
                                       T_AMOUNT     ,
                                       T_SHORT      ,
                                       T_RETFLAG    
                                     )
                               VALUES( BuyLot.t_Client,               --T_CLIENT     
                                       BuyLot.t_Contract,             --T_CONTRACT   
                                       p_GO.t_FIID,                   --T_FIID       
                                       BuyLot.t_ID,                   --T_BUYID
                                       v_SALE_ID,                     --T_SALEID
                                       0,                             --T_SOURCEID
                                       RSI_NPTXC.NPTXLNK_DELIVER,     --T_TYPE       
                                       p_GO.t_SaleDate,               --T_DATE    
                                       v_Amount,                      --T_AMOUNT     
                                       0,                             --T_SHORT
                                       CHR(0)                         --T_RETFLAG
                                     ) RETURNING t_ID INTO v_LCS_ID;

            UPDATE dnptxlot_dbt
               SET t_ClGOID  = p_GO.t_ID
             WHERE t_ID = BuyLot.t_ID;

             v_ExistSale := True;

         END IF;
      END LOOP;

      IF v_ExistSale = TRUE THEN
        SELECT * INTO v_Fin
          FROM DFININSTR_DBT
         WHERE t_FIID = p_GO.T_FIID;

        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Произведено списание ц/б '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                                            ' в рамках обработки '||iif(p_GO.t_DocKind = RSI_NPTXC.DL_CONVAVR, 'конвертации', 'корпоративных действий')||' по операции № '||p_GO.T_CODE||' на дату '||TO_CHAR(p_GO.T_SALEDATE,'DD.MM.YYYY') );
      END IF;

      IF p_GO.T_KIND = RSI_NPTXC.NPTXLOTORIGIN_IN THEN
         ProcessGON(p_GO, pClient, v_IIS, pContract);  
      END IF;
    END; --ProcessGO

    --Выполняет зачисление лотов в ГО
    PROCEDURE ProcessGOFI(p_GO IN DNPTXGO_DBT%ROWTYPE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0)  
    IS
      v_Count  NUMBER := 0;
      v_N      NUMBER := 0;
      v_KC     NUMBER := 0;
      v_Fin    DFININSTR_DBT%ROWTYPE;
      v_Nnew NUMBER := 0;
      v_Nold NUMBER := 0;
      v_Cnew NUMBER := 0;
      v_OldAmount NUMBER := 0;
      v_NewAmount NUMBER := 0;
      v_CFI    DDL_LEG_DBT.T_CFI%TYPE;
      v_Cold   DDL_LEG_DBT.T_TOTALCOST%TYPE;
      v_Price  NUMBER := 0;
      v_NewLot    DNPTXLOT_DBT%ROWTYPE;
      v_CalcAmount NUMBER := 0;
      v_CalcAmount_new NUMBER := 0;
      v_NKD        NUMBER := 0;
      v_Character   VARCHAR2(40);
      v_BuyDate  DATE;
      v_SaleDate DATE;
      v_BuyID  NUMBER := 0;
      v_SaleID NUMBER := 0;
      N    NUMBER := 0;
      v_ConsiderFirstBuyDate CHAR(1) := CHR(0); 
      v_CorpActionDate       DATE    := TO_DATE('01.01.0001','DD.MM.YYYY');
      v_SumPrecision         NUMBER  := 0;

      CURSOR cGOFI(v_GOID IN NUMBER) IS
      SELECT *
        FROM DNPTXGOFI_DBT
       WHERE T_GOID = v_GOID;

      CURSOR cL IS
             SELECT lot.*, 
                    (SELECT NVL(SUM(T.t_Amount), 0)
                       FROM DNPTXTS_DBT T
                      WHERE T.t_BuyID = lot.T_ID
                        AND T.t_Type IN (RSI_NPTXC.NPTXTS_REST, RSI_NPTXC.NPTXTS_INVST)
                        AND T.t_BegDate <= p_GO.T_SaleDate
                        AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                    ) OldAmount
               FROM DNPTXLOT_DBT lot
              WHERE lot.T_KIND = RSI_NPTXC.NPTXLOTS_BUY
                AND lot.T_CLGOID = p_GO.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end
                AND (SELECT NVL(SUM(T.t_Amount), 0)
                       FROM DNPTXTS_DBT T
                      WHERE T.t_BuyID = lot.T_ID
                        AND T.t_Type IN (RSI_NPTXC.NPTXTS_REST, RSI_NPTXC.NPTXTS_INVST)
                        AND T.t_BegDate <= p_GO.T_SaleDate
                        AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                    ) > 0;

      CURSOR cL2 IS
             SELECT lot.*, 
                    (SELECT NVL(SUM(T.t_Amount), 0)
                       FROM DNPTXTS_DBT T
                      WHERE T.t_SaleID = lot.T_ID
                        AND T.t_Type = RSI_NPTXC.NPTXTS_SHPOS
                        AND T.t_BegDate <= p_GO.T_SaleDate
                        AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                    ) OldAmount
               FROM DNPTXLOT_DBT lot
              WHERE lot.T_KIND = RSI_NPTXC.NPTXLOTS_SALE
                AND lot.T_CLGOID = p_GO.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS   
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end
                AND (SELECT NVL(SUM(T.t_Amount), 0)
                       FROM DNPTXTS_DBT T
                      WHERE T.t_SaleID = lot.T_ID
                        AND T.t_Type = RSI_NPTXC.NPTXTS_SHPOS
                        AND T.t_BegDate <= p_GO.T_SaleDate
                        AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                    ) > 0;

      CURSOR cL3 IS
             SELECT lot.*
               FROM DNPTXLOT_DBT lot
              WHERE lot.T_KIND IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_BACKREPO,
                                   RSI_NPTXC.NPTXLOTS_LOANPUT, RSI_NPTXC.NPTXLOTS_LOANGET)
                AND lot.T_CLGOID = p_GO.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end;

      CURSOR cRest1 IS
             SELECT lot.t_Client, lot.t_Contract, T.t_BUYID, T.t_SALEID, T.t_Amount
               FROM DNPTXLOT_DBT lot, DNPTXTS_DBT T
              WHERE lot.T_KIND = RSI_NPTXC.NPTXLOTS_SALE
                AND lot.T_CLGOID = p_GO.T_ID
                AND T.t_SaleID = lot.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS 
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end
                AND T.t_Type = RSI_NPTXC.NPTXTS_SHPOS
                AND T.t_BegDate <= p_GO.T_SaleDate
                AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                AND T.t_Amount > 0;

      CURSOR cRest2 IS
             SELECT lot.t_Client, lot.t_Contract, T.t_BUYID, T.t_SALEID, T.t_Amount
               FROM DNPTXLOT_DBT lot, DNPTXTS_DBT T
              WHERE lot.T_KIND = RSI_NPTXC.NPTXLOTS_BUY
                AND lot.T_CLGOID = p_GO.T_ID
                AND lot.T_CLIENT = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS 
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end
                AND T.t_BuyID = lot.T_ID
                AND T.t_Type = RSI_NPTXC.NPTXTS_INVST
                AND T.t_BegDate <= p_GO.T_SaleDate
                AND T.t_EndDate  = p_GO.T_SaleDate --закрыто датой списания
                AND T.t_Amount > 0;

    BEGIN

      select TRIM(VALUE) INTO v_Character from nls_session_parameters where parameter = 'NLS_NUMERIC_CHARACTERS';

      SELECT COUNT(1) INTO v_N
        FROM DNPTXGOFI_DBT GF
       WHERE GF.T_GOID = p_GO.T_ID;

      IF v_N > 1 THEN
        SELECT NVL(SUM(TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character))), 0) INTO v_KC
          FROM DNPTXGOFI_DBT F
         WHERE F.T_GOID = p_GO.T_ID;
      END IF;

      BEGIN
        SELECT NVL(comm.t_ConsiderFirstBuyDate, CHR(0)), NVL(comm.t_CorpActionDate, TO_DATE('01.01.0001','DD.MM.YYYY'))
          INTO v_ConsiderFirstBuyDate, v_CorpActionDate 
          FROM DDL_COMM_DBT comm 
         WHERE comm.T_DOCKIND = p_GO.T_DOCKIND 
           AND comm.T_DOCUMENTID = p_GO.T_DOCUMENTID;

        EXCEPTION 
          WHEN NO_DATA_FOUND THEN
            v_ConsiderFirstBuyDate := CHR(0);                            
            v_CorpActionDate       := TO_DATE('01.01.0001','DD.MM.YYYY');
      END;

      FOR F IN cGOFI(p_GO.T_ID)
      LOOP
        v_Nold := 0;
        v_Nnew := 0;
        
        SELECT * INTO v_Fin
          FROM DFININSTR_DBT
         WHERE t_FIID = F.T_NEWFIID;

        BEGIN
          SELECT NVL(lfi.t_SumPrecision, fin.t_SumPrecision)
            INTO v_SumPrecision
            FROM dscdlfi_dbt lfi, dfininstr_dbt fin
           WHERE lfi.t_DealKind = p_GO.t_DocKind
             AND lfi.t_DealID = p_GO.t_DocumentID
             AND lfi.t_NewFIID = F.T_NEWFIID
             AND fin.t_FIID = lfi.t_NewFIID
             AND ROWNUM = 1;

          EXCEPTION 
            WHEN NO_DATA_FOUND THEN v_SumPrecision := 0;
        END;

        FOR L IN cL LOOP
          v_OldAmount := L.OldAmount;
          v_NewAmount := ROUND(v_OldAmount * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character)), v_SumPrecision);
          v_Nold      := v_Nold + v_OldAmount;
          v_Nnew      := v_Nnew + v_NewAmount;

          IF L.T_DOCKIND = RSI_NPTXC.DL_SECURITYDOC THEN
             SELECT leg.T_CFI, leg.T_TOTALCOST * v_OldAmount / L.t_Amount
               INTO v_CFI,     v_Cold
               FROM ddl_leg_dbt leg
              WHERE leg.T_LEGKIND = 0
                AND leg.T_DEALID = L.T_DOCID
                AND leg.T_LEGID = 0;
          ELSE
             v_CFI  := L.t_PriceFIID;
             v_Cold := L.T_TOTALCOST * v_OldAmount / L.t_Amount;
          END IF;

          IF v_N > 1 THEN
            v_Cnew := v_Cold * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character ))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character ))/v_KC;
          ELSE
            v_Cnew := v_Cold;
          END IF;

          v_NKD := RSI_RSB_FIInstr.FI_CalcNKD(F.T_NEWFIID, p_GO.T_BUYDATE, v_NewAmount, 0);

          IF v_NKD <> 0 AND v_Fin.T_FACEVALUEFI <> v_CFI THEN
            v_NKD := RSI_RSB_FIInstr.ConvSum( v_NKD, v_Fin.T_FACEVALUEFI, v_CFI, p_GO.T_BUYDATE );
          END IF;

          IF v_NewAmount > 0 THEN
            v_Price := (v_Cnew - v_NKD)/v_NewAmount;
          END IF;

          INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                     T_DOCID          ,
                                     T_RQID           ,
                                     T_DEALDATE       ,
                                     T_DEALTIME       ,
                                     T_DEALCODE       ,
                                     T_DEALCODETS     ,
                                     T_CLIENT         ,
                                     T_CONTRACT       ,
                                     T_FIID           ,
                                     T_KIND           ,
                                     T_TYPE           ,
                                     T_BUYID          ,
                                     T_REALID         ,
                                     T_AMOUNT         ,
                                     T_SALE           ,
                                     T_COMPAMOUNT     ,
                                     T_PRICE          ,
                                     T_PRICEFIID      ,
                                     T_TOTALCOST      ,
                                     T_NKD            ,
                                     T_BUYDATE        ,
                                     T_SALEDATE       ,
                                     T_SORTCODE       ,
                                     T_RETFLAG        ,
                                     T_ISFREE         ,
                                     T_BEGLOTID       ,
                                     T_CHILDID        ,
                                     T_BEGBUYDATE     ,
                                     T_BEGSALEDATE    ,
                                     T_INACC          ,
                                     T_BLOCKED        ,
                                     T_ORIGIN         ,
                                     T_GOID           ,
                                     T_CLGOID         ,
                                     T_OLDDATE        ,
                                     T_ORDFORSALE     ,
                                     T_ORDFORREPO     ,
                                     T_NOTCOUNTEDONIIS
                                   )
                             VALUES( L.t_DocKind,                                --T_DOCKIND    
                                     L.t_DocID,                                  --T_DOCID      
                                     L.t_RQID,                                   --T_RQID  
                                     L.t_DealDate,                               --T_DEALDATE
                                     L.t_DealTime,                               --T_DEALTIME
                                     L.t_DealCode,                               --T_DEALCODE
                                     L.t_DealCodeTS,                             --T_DEALCODETS
                                     L.t_Client,                                 --T_CLIENT     
                                     L.t_Contract,                               --T_CONTRACT   
                                     F.T_NEWFIID,                                --T_FIID       
                                     RSI_NPTXC.NPTXLOTS_BUY,                     --T_KIND       
                                     RSI_NPTXC.NPTXDEAL_REAL,                    --T_TYPE       
                                     0,                                          --T_BUYID
                                     L.t_ID,                                     --T_REALID
                                     v_NewAmount,                                --T_AMOUNT     
                                     0,                                          --T_SALE     
                                     0,                                          --T_COMPAMOUNT     
                                     v_Price,                                    --T_PRICE    
                                     v_CFI,                                      --T_PRICEFIID
                                     v_Cnew,                                     --T_TOTALCOST
                                     v_NKD,                                      --T_NKD      
                                     L.T_BUYDATE,                                --T_BUYDATE    
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_SALEDATE   
                                     L.t_SortCode,                               --T_SORTCODE
                                     CHR(0),                                     --T_RETFLAG
                                     CHR(0),                                     --T_ISFREE
                                     L.t_BegLotID,                               --T_BEGLOTID
                                     0,                                          --T_CHILDID 
                                     (CASE WHEN v_ConsiderFirstBuyDate = 'X' THEN L.T_BUYDATE ELSE v_CorpActionDate END),      --T_BEGBUYDATE
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BEGSALEDATE   
                                     CHR(88),                                    --T_INACC
                                     CHR(0),                                     --T_BLOCKED
                                     RSI_NPTXC.NPTXLOTORIGIN_GO,                 --T_ORIGIN
                                     p_GO.T_ID,                                  --T_GOID  
                                     0,                                          --T_CLGOID 
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_OLDDATE   
                                     0,                                          --T_ORDFORSALE
                                     0,                                          --T_ORDFORREPO
                                     RSI_NPTX.DetermineIISCountingStatus(L.t_DocKind, L.t_DocID, L.t_Contract) --T_NOTCOUNTEDONIIS
                                   );

          IF N = 0 THEN
            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Произведено зачисление ц/б '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                                              ' в рамках обработки '||iif(p_GO.t_DocKind = RSI_NPTXC.DL_CONVAVR, 'конвертации', 'корпоративных действий')||' по операции № '||p_GO.T_CODE||' на дату '||TO_CHAR(p_GO.T_BUYDATE,'DD.MM.YYYY') );
          END IF;
          N := N + 1;

          RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'По сделке № '||L.T_DEALCODETS||' '||v_NewAmount||' бумаг' || ' стоимостью ' || v_Cnew   );
        END LOOP;

        FOR L IN cL2 LOOP
          v_OldAmount := L.OldAmount;
          v_NewAmount := v_OldAmount * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character));
          v_Nold      := v_Nold + v_OldAmount;
          v_Nnew      := v_Nnew + v_NewAmount;

          IF L.T_DOCKIND = RSI_NPTXC.DL_SECURITYDOC THEN
             SELECT leg.T_CFI, leg.T_TOTALCOST * v_OldAmount / L.t_Amount
               INTO v_CFI,     v_Cold
               FROM ddl_leg_dbt leg
              WHERE leg.T_LEGKIND = 0
                AND leg.T_DEALID = L.T_DOCID
                AND leg.T_LEGID = 0;
          ELSE
             v_CFI  := L.t_PriceFIID;
             v_Cold := L.T_TOTALCOST * v_OldAmount / L.t_Amount;
          END IF;

          IF v_N > 1 THEN
            v_Cnew := v_Cold * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character ))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character ))/v_KC;
          ELSE
            v_Cnew := v_Cold;
          END IF;

          v_NKD := RSI_RSB_FIInstr.FI_CalcNKD(F.T_NEWFIID, p_GO.T_BUYDATE, v_NewAmount, 0);

          IF v_NKD <> 0 AND v_Fin.T_FACEVALUEFI <> v_CFI THEN
            v_NKD := RSI_RSB_FIInstr.ConvSum( v_NKD, v_Fin.T_FACEVALUEFI, v_CFI, p_GO.T_BUYDATE );
          END IF;

          v_Price := (v_Cnew - v_NKD)/v_NewAmount;

          INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                     T_DOCID          ,
                                     T_RQID           ,
                                     T_DEALDATE       ,
                                     T_DEALTIME       ,
                                     T_DEALCODE       ,
                                     T_DEALCODETS     ,
                                     T_CLIENT         ,
                                     T_CONTRACT       ,
                                     T_FIID           ,
                                     T_KIND           ,
                                     T_TYPE           ,
                                     T_BUYID          ,
                                     T_REALID         ,
                                     T_AMOUNT         ,
                                     T_SALE           ,
                                     T_COMPAMOUNT     ,
                                     T_PRICE          ,
                                     T_PRICEFIID      ,
                                     T_TOTALCOST      ,
                                     T_NKD            ,
                                     T_BUYDATE        ,
                                     T_SALEDATE       ,
                                     T_SORTCODE       ,
                                     T_RETFLAG        ,
                                     T_ISFREE         ,
                                     T_BEGLOTID       ,
                                     T_CHILDID        ,
                                     T_BEGBUYDATE     ,
                                     T_BEGSALEDATE    ,
                                     T_INACC          ,
                                     T_BLOCKED        ,
                                     T_ORIGIN         ,
                                     T_GOID           ,
                                     T_CLGOID         ,
                                     T_OLDDATE        ,
                                     T_ORDFORSALE     ,
                                     T_ORDFORREPO     ,
                                     T_NOTCOUNTEDONIIS
                                   )
                             VALUES( L.t_DocKind,                                --T_DOCKIND    
                                     L.t_DocID,                                  --T_DOCID      
                                     L.t_RQID,                                   --T_RQID  
                                     L.t_DealDate,                               --T_DEALDATE
                                     L.t_DealTime,                               --T_DEALTIME
                                     L.t_DealCode,                               --T_DEALCODE
                                     L.t_DealCodeTS,                             --T_DEALCODETS
                                     L.t_Client,                                 --T_CLIENT     
                                     L.t_Contract,                               --T_CONTRACT   
                                     F.T_NEWFIID,                                --T_FIID       
                                     RSI_NPTXC.NPTXLOTS_SALE,                    --T_KIND       
                                     RSI_NPTXC.NPTXDEAL_REAL,                    --T_TYPE       
                                     0,                                          --T_BUYID
                                     L.t_ID,                                     --T_REALID
                                     v_NewAmount,                                --T_AMOUNT     
                                     0,                                          --T_SALE     
                                     0,                                          --T_COMPAMOUNT     
                                     v_Price,                                    --T_PRICE    
                                     v_CFI,                                      --T_PRICEFIID
                                     v_Cnew,                                     --T_TOTALCOST
                                     v_NKD,                                      --T_NKD      
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BUYDATE    
                                     p_GO.t_BuyDate,                             --T_SALEDATE   
                                     L.t_SortCode,                               --T_SORTCODE
                                     CHR(0),                                     --T_RETFLAG
                                     CHR(0),                                     --T_ISFREE
                                     L.t_BegLotID,                               --T_BEGLOTID
                                     0,                                          --T_CHILDID 
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_BEGBUYDATE    
                                     L.t_BegSaleDate,                            --T_BEGSALEDATE   
                                     CHR(88),                                    --T_INACC
                                     CHR(0),                                     --T_BLOCKED
                                     RSI_NPTXC.NPTXLOTORIGIN_GO,                 --T_ORIGIN
                                     p_GO.T_ID,                                  --T_GOID  
                                     0,                                          --T_CLGOID 
                                     TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_OLDDATE   
                                     0,                                          --T_ORDFORSALE
                                     0,                                          --T_ORDFORREPO
                                     RSI_NPTX.DetermineIISCountingStatus(L.t_DocKind, L.t_DocID, L.t_Contract) --T_NOTCOUNTEDONIIS
                                   );

          RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'По сделке № '||L.T_DEALCODETS||' '||v_NewAmount||' бумаг'||' стоимостью ' || v_Cnew );
        END LOOP;

        FOR L IN cL3 LOOP
          IF(L.T_OLDDATE < p_GO.T_BUYDATE AND L.T_OLDDATE <> TO_DATE('01.01.0001','DD.MM.YYYY') AND L.T_OLDDATE IS NOT NULL) THEN
             RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Дата второй части '||L.T_OLDDATE||' операции Репо '||L.T_DEALCODETS||' меньше даты зачисления '||p_GO.T_BUYDATE||' операции конвертации '||p_GO.T_CODE );
          ELSE
             v_OldAmount := L.t_Amount;
             v_NewAmount := v_OldAmount * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character));
             v_Nold      := v_Nold + v_OldAmount;
             v_Nnew      := v_Nnew + v_NewAmount;

             IF L.T_DOCKIND = RSI_NPTXC.DL_SECURITYDOC THEN
                SELECT leg.T_CFI, leg.T_TOTALCOST * v_OldAmount / L.t_Amount
                  INTO v_CFI,     v_Cold
                  FROM ddl_leg_dbt leg
                 WHERE leg.T_LEGKIND = 0
                   AND leg.T_DEALID = L.T_DOCID
                   AND leg.T_LEGID = 0;
             ELSE
                v_CFI  := L.t_PriceFIID;
                v_Cold := L.T_TOTALCOST * v_OldAmount / L.t_Amount;
             END IF;

             IF v_N > 1 THEN
               v_Cnew := v_Cold * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character ))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character ))/v_KC;
             ELSE
               v_Cnew := v_Cold;
             END IF;

             v_NKD := RSI_RSB_FIInstr.FI_CalcNKD(F.T_NEWFIID, p_GO.T_BUYDATE, v_NewAmount, 0);

             IF v_NKD <> 0 AND v_Fin.T_FACEVALUEFI <> v_CFI THEN
               v_NKD := RSI_RSB_FIInstr.ConvSum( v_NKD, v_Fin.T_FACEVALUEFI, v_CFI, p_GO.T_BUYDATE );
             END IF;

             v_Price := (v_Cnew - v_NKD)/v_NewAmount;

             IF (L.t_Kind IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)) THEN
                v_BuyDate  := L.T_OLDDATE;
                v_SaleDate := p_GO.T_BUYDATE;
             ELSE
                v_BuyDate  := p_GO.T_BUYDATE;
                v_SaleDate := L.T_OLDDATE;
             END IF;

             INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                        T_DOCID          ,
                                        T_RQID           ,
                                        T_DEALDATE       ,
                                        T_DEALTIME       ,
                                        T_DEALCODE       ,
                                        T_DEALCODETS     ,
                                        T_CLIENT         ,
                                        T_CONTRACT       ,
                                        T_FIID           ,
                                        T_KIND           ,
                                        T_TYPE           ,
                                        T_BUYID          ,
                                        T_REALID         ,
                                        T_AMOUNT         ,
                                        T_SALE           ,
                                        T_COMPAMOUNT     ,
                                        T_PRICE          ,
                                        T_PRICEFIID      ,
                                        T_TOTALCOST      ,
                                        T_NKD            ,
                                        T_BUYDATE        ,
                                        T_SALEDATE       ,
                                        T_SORTCODE       ,
                                        T_RETFLAG        ,
                                        T_ISFREE         ,
                                        T_BEGLOTID       ,
                                        T_CHILDID        ,
                                        T_BEGBUYDATE     ,
                                        T_BEGSALEDATE    ,
                                        T_INACC          ,
                                        T_BLOCKED        ,
                                        T_ORIGIN         ,
                                        T_GOID           ,
                                        T_CLGOID         ,
                                        T_OLDDATE        ,
                                        T_ORDFORSALE     ,
                                        T_ORDFORREPO     ,
                                        T_NOTCOUNTEDONIIS
                                      )
                                VALUES( L.t_DocKind,                                --T_DOCKIND    
                                        L.t_DocID,                                  --T_DOCID      
                                        L.t_RQID,                                   --T_RQID  
                                        L.t_DealDate,                               --T_DEALDATE
                                        L.t_DealTime,                               --T_DEALTIME
                                        L.t_DealCode,                               --T_DEALCODE
                                        L.t_DealCodeTS,                             --T_DEALCODETS
                                        L.t_Client,                                 --T_CLIENT     
                                        L.t_Contract,                               --T_CONTRACT   
                                        F.T_NEWFIID,                                --T_FIID       
                                        L.t_Kind,                                   --T_KIND       
                                        RSI_NPTXC.NPTXDEAL_REAL,                    --T_TYPE       
                                        0,                                          --T_BUYID
                                        L.t_ID,                                     --T_REALID
                                        v_NewAmount,                                --T_AMOUNT     
                                        0,                                          --T_SALE     
                                        0,                                          --T_COMPAMOUNT     
                                        v_Price,                                    --T_PRICE    
                                        v_CFI,                                      --T_PRICEFIID
                                        v_Cnew,                                     --T_TOTALCOST
                                        v_NKD,                                      --T_NKD      
                                        v_BuyDate,                                  --T_BUYDATE    
                                        v_SaleDate,                                 --T_SALEDATE   
                                        L.t_SortCode,                               --T_SORTCODE
                                        CHR(0),                                     --T_RETFLAG
                                        CHR(0),                                     --T_ISFREE
                                        L.t_BegLotID,                               --T_BEGLOTID
                                        0,                                          --T_CHILDID 
                                        L.t_BegBuyDate,                             --T_BEGBUYDATE    
                                        L.t_BegSaleDate,                            --T_BEGSALEDATE   
                                        CHR(88),                                    --T_INACC
                                        L.t_Blocked,                                --T_BLOCKED
                                        RSI_NPTXC.NPTXLOTORIGIN_GO,                 --T_ORIGIN
                                        p_GO.T_ID,                                  --T_GOID  
                                        0,                                          --T_CLGOID 
                                        TO_DATE('01.01.0001','DD.MM.YYYY'),         --T_OLDDATE   
                                        0,                                          --T_ORDFORSALE
                                        0,                                          --T_ORDFORREPO
                                        RSI_NPTX.DetermineIISCountingStatus(L.t_DocKind, L.t_DocID, L.t_Contract) --T_NOTCOUNTEDONIIS
                                      );

             RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'По сделке № '||L.T_DEALCODETS||' '||v_NewAmount||' бумаг' ||' стоимостью ' || v_Cnew  );
          END IF;
        END LOOP;

        v_CalcAmount := round(v_Nold * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character ))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character )), v_Fin.T_SUMPRECISION);
        v_CalcAmount_new := round(v_Nnew * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character ))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character )), v_Fin.T_SUMPRECISION);

        IF v_CalcAmount_new <> v_CalcAmount THEN
          RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_INF, 'Общее количество ц/б по лотам выпуска '||v_Fin.T_FI_CODE||' '||v_Fin.T_NAME||
                                                ' в системе '||v_NewAmount||' штук, расчетное количество по операции '||v_CalcAmount||' штук, необходимо провести коррекцию количества на налоговых лотах');
        END IF;

        FOR Rest1 IN cRest1 LOOP
          
           BEGIN
              SELECT t_ID 
                INTO v_BuyID
                FROM dnptxlot_dbt
               WHERE t_GOID = p_GO.T_ID
                 AND t_RealID = Rest1.t_BUYID
                 AND t_FIID = F.T_NEWFIID
                 AND t_Kind IN (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET)
                 AND ROWNUM = 1;
           EXCEPTION
             WHEN NO_DATA_FOUND
               THEN
                 BEGIN
                   v_BuyID := 0;
                 END;
           END;

           BEGIN
              SELECT t_ID 
                INTO v_SaleID
                FROM dnptxlot_dbt
               WHERE t_GOID = p_GO.T_ID
                 AND t_RealID = Rest1.t_SALEID
                 AND t_FIID = F.T_NEWFIID
                 AND t_Kind = RSI_NPTXC.NPTXLOTS_SALE
                 AND ROWNUM = 1;
           EXCEPTION
             WHEN NO_DATA_FOUND
               THEN
                 BEGIN
                   v_SaleID := 0;
                 END;
           END;

           INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                      T_CONTRACT   ,
                                      T_FIID       ,
                                      T_BUYID      ,
                                      T_SALEID     ,
                                      T_SOURCEID   ,
                                      T_TYPE       ,
                                      T_DATE       ,
                                      T_AMOUNT     ,
                                      T_SHORT      ,
                                      T_RETFLAG    
                                    )
                              VALUES( Rest1.t_Client,                --T_CLIENT     
                                      Rest1.t_Contract,              --T_CONTRACT   
                                      F.T_NEWFIID,                   --T_FIID       
                                      v_BuyID,                       --T_BUYID
                                      v_SaleID,                      --T_SALEID
                                      0,                             --T_SOURCEID
                                      RSI_NPTXC.NPTXLNK_OPPOS,       --T_TYPE       
                                      p_GO.t_BuyDate,                --T_DATE    
                                      Rest1.t_Amount * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character)), --T_AMOUNT     
                                      0,                             --T_SHORT
                                      CHR(0)                         --T_RETFLAG
                                    );
        END LOOP;


        FOR Rest2 IN cRest2 LOOP
          
           BEGIN
              SELECT t_ID 
                INTO v_BuyID
                FROM dnptxlot_dbt
               WHERE t_GOID = p_GO.T_ID
                 AND t_RealID = Rest2.t_BUYID
                 AND t_FIID = F.T_NEWFIID
                 AND t_Kind = RSI_NPTXC.NPTXLOTS_BUY
                 AND ROWNUM = 1;
           EXCEPTION
             WHEN NO_DATA_FOUND
               THEN
                 BEGIN
                   v_BuyID := 0;
                 END;
           END;

           BEGIN
              SELECT t_ID 
                INTO v_SaleID
                FROM dnptxlot_dbt
               WHERE t_GOID = p_GO.T_ID
                 AND t_RealID = Rest2.t_SALEID
                 AND t_FIID = F.T_NEWFIID
                 AND t_Kind IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)
                 AND ROWNUM = 1;
           EXCEPTION
             WHEN NO_DATA_FOUND
               THEN
                 BEGIN
                   v_SaleID := 0;
                 END;
           END;

           INSERT INTO dnptxlnk_dbt ( T_CLIENT     ,
                                      T_CONTRACT   ,
                                      T_FIID       ,
                                      T_BUYID      ,
                                      T_SALEID     ,
                                      T_SOURCEID   ,
                                      T_TYPE       ,
                                      T_DATE       ,
                                      T_AMOUNT     ,
                                      T_SHORT      ,
                                      T_RETFLAG    
                                    )
                              VALUES( Rest2.t_Client,                --T_CLIENT     
                                      Rest2.t_Contract,              --T_CONTRACT   
                                      F.T_NEWFIID,                   --T_FIID       
                                      v_BuyID,                       --T_BUYID
                                      v_SaleID,                      --T_SALEID
                                      0,                             --T_SOURCEID
                                      RSI_NPTXC.NPTXLNK_REPO,        --T_TYPE       
                                      p_GO.t_BuyDate,                --T_DATE    
                                      Rest2.t_Amount * TO_NUMBER(REPLACE(F.T_NUMERATOR,'.',v_Character))/TO_NUMBER(REPLACE(F.T_DENOMINATOR,'.',v_Character)), --T_AMOUNT     
                                      0,                             --T_SHORT
                                      CHR(0)                         --T_RETFLAG
                                    );
        END LOOP;
      END LOOP;
    END;


    -- Процедура вставки лотов
    PROCEDURE InsertLots( pBegDate IN DATE, pEndDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0 )
    IS
       v_Count NUMBER;
       v_LotID NUMBER;
       v_BuyDate     DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
       v_SaleDate    DATE := TO_DATE('01.01.0001','DD.MM.YYYY');
       v_BegBuyDate  DATE;
       v_BegSaleDate DATE;
       v_Date2       DATE;
       v_Amount      NUMBER := 0;
       v_RestPaymAmount NUMBER;
       v_Break       BOOLEAN;
       clot dnptxlot_dbt%ROWTYPE;
       v_rqamount         NUMBER;
       v_Kind             NUMBER;
       v_dealcode         ddl_tick_dbt.t_dealcode%TYPE;
       v_dealcodets       ddl_tick_dbt.t_dealcodets%TYPE;
       v_dealdate         ddl_tick_dbt.t_dealdate%TYPE;
       v_dealtime         ddl_tick_dbt.t_dealtime%TYPE;
       v_ClientContrID    ddl_tick_dbt.t_ClientContrID%TYPE;
       v_legprice2        ddl_leg_dbt.t_Price%TYPE;
       v_legTotalCost2    ddl_leg_dbt.t_TotalCost%TYPE;
       v_legNKD2          ddl_leg_dbt.t_NKD%TYPE;
       issale             NUMBER;
       isbuy              NUMBER;
       ogrp               NUMBER;
       v_legcfi2          NUMBER;
       v_CompRQ           ddlrq_dbt%ROWTYPE;

       CURSOR BackPaym IS SELECT RQ.t_FactDate, TXLot.t_Kind, TXLot.t_ID, TXLot.t_DealCode, TXLot.t_BegLotID
                            FROM ddlrq_dbt RQ, dnptxlot_dbt TXLot
                           WHERE RQ.t_DocKind = RSI_NPTXC.DL_SECURITYDOC
                             AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
                             AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                             AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                             AND RQ.t_DealPart= 2
                             AND RQ.t_FactDate >= pBegDate
                             AND RQ.t_FactDate <= pEndDate
                             AND TXLot.t_DocKind = RSI_NPTXC.DL_SECURITYDOC
                             AND TXLot.t_DocID = RQ.t_DocID
                             AND TXLot.t_Client = pClient
                             AND TXLot.t_FIID = case when pFIID != -1 then pFIID else TXLot.t_FIID end
                             AND RSI_NPTO.CheckContrIIS(TXLot.t_Contract)  = pIIS
                             AND ((pContract IS NULL) OR (pContract <= 0) OR (TXLot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                             AND TXLot.t_Kind IN ( RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANPUT, RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANGET )
                             AND TXLot.t_Type = RSI_NPTXC.NPTXDEAL_REAL
                           ORDER BY TXLot.t_DocID ASC, RQ.t_FactDate ASC;

       CURSOR CompPaym IS SELECT sum(CASE WHEN rq.t_kind = rsi_dlrq.dlrq_kind_commit THEN rq.t_Amount ELSE -rq.t_Amount END) SumAmount, rq.t_fiid, rq.t_docid, rq.t_factdate
                            FROM ddlrq_dbt rq,
                                 ddl_tick_dbt tick
                           WHERE rq.t_dockind = RSI_NPTXC.DL_SECURITYDOC
                             AND rq.t_state = rsi_dlrq.dlrq_state_exec
                             AND rq.t_subkind = rsi_dlrq.dlrq_subkind_avoiriss
                             AND rq.t_type = rsi_dlrq.dlrq_type_compdelivery
                             AND rq.t_fiid = case when pFIID != -1 then pFIID else rq.t_fiid end
                             AND rq.t_factdate >= pBegDate
                             AND rq.t_factdate <= pEndDate
                             AND tick.t_dealid = rq.t_docid
                             AND ( (tick.t_ClientID = pClient AND RSI_NPTO.CheckContrIIS(tick.t_ClientContrID) = pIIS 
                                    AND ((pContract IS NULL) OR (pContract <= 0) OR (tick.t_ClientContrID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                    ) 
                                  OR (tick.t_PartyID = pClient AND tick.t_IsPartyClient = 'X' AND RSI_NPTO.CheckContrIIS(tick.t_PartyContrID) = pIIS 
                                      AND ((pContract IS NULL) OR (pContract <= 0) OR (tick.t_PartyContrID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                      )
                                  )
                        GROUP BY rq.t_docid, rq.t_fiid, rq.t_factdate
                        ORDER BY rq.t_docid ASC, rq.t_fiid ASC, rq.t_factdate ASC;
    BEGIN
       --Занести лоты покупок/продаж, совершенных за период.
       INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                  T_DOCID          ,
                                  T_RQID           ,
                                  T_DEALDATE       ,
                                  T_DEALTIME       ,
                                  T_DEALCODE       ,
                                  T_DEALCODETS     ,
                                  T_CLIENT         ,
                                  T_CONTRACT       ,
                                  T_FIID           ,
                                  T_KIND           ,
                                  T_AMOUNT         ,
                                  T_PRICE          ,
                                  T_PRICEFIID      ,
                                  T_TOTALCOST      ,
                                  T_NKD            ,
                                  T_BUYDATE        ,
                                  T_SALEDATE       ,
                                  T_BLOCKED        ,
                                  T_NOTCOUNTEDONIIS)
                           SELECT RQ.t_DocKind,                                                              --T_DOCKIND    
                                  RQ.t_DocID,                                                                --T_DOCID      
                                  RQ.t_ID,                                                                   --T_RQID  
                                  Tick.t_DealDate,                                                           --T_DEALDATE
                                  Tick.t_DealTime,                                                           --T_DEALTIME
                                  RTRIM(Tick.t_DealCode),                                                    --T_DEALCODE
                                  RTRIM(DECODE(Tick.t_DealCodeTS,CHR(1),Tick.t_DealCode,Tick.t_DealCodeTS)), --T_DEALCODETS
                                  pClient,                                                                   --T_CLIENT     
                                  Tick.t_ClientContrID,                                                      --T_CONTRACT   
                                  Leg.t_PFI,                                                                 --T_FIID       
                                  get_lotKind(Opr.oGrp, Tick.t_DealID, RQ.t_DealPart, 0),                    --T_KIND       
                                  RQ.t_Amount,                                                               --T_AMOUNT     
                                  (CASE WHEN Leg.t_RelativePrice = CHR(0) OR
                                             Leg.t_RelativePrice IS NULL OR
                                             RQ.t_DocKind = RSI_NPTXC.DL_RETIREMENT THEN Leg.t_Price
                                        ELSE RSI_RSB_FIInstr.FI_GetNominalOnDate(Leg.t_PFI, 
                                             RQ.t_FactDate)*Leg.t_Price/100 END),                            --T_PRICE
                                  Leg.t_CFI,                                                                 --T_PRICEFIID
                                  Leg.t_TotalCost*RQ.t_Amount/Leg.t_Principal,                               --T_TOTALCOST  
                                  Leg.t_NKD*RQ.t_Amount/Leg.t_Principal,                                     --T_NKD        
                                  get_lotBuyDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart, 0),  --T_BUYDATE       
                                  get_lotSaleDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart, 0), --T_SALEDATE       
                                   Tick.T_BLOCKED, --T_BLOCKED
                                  RSI_NPTX.DetermineIISCountingStatus(RQ.t_DocKind, RQ.t_DocID, Tick.t_ClientContrID) --T_NOTCOUNTEDONIIS
                             FROM ddlrq_dbt RQ, ddl_tick_dbt Tick, ddl_leg_dbt Leg,
                                  (SELECT t_Kind_Operation, t_DocKind, rsb_secur.get_OperationGroup(t_SysTypes) oGrp
                                     FROM doprkoper_dbt) Opr
                            WHERE Tick.t_DealID = RQ.t_DocID
                              AND Tick.t_BOfficeKind = RQ.t_DocKind
                              AND Opr.t_Kind_Operation = Tick.t_DealType
                              AND Opr.t_DocKind = Tick.t_BOfficeKind
                              AND Leg.t_DealID = Tick.t_DealID
                              AND Leg.t_LegID = 0
                              AND Leg.t_LegKind = DECODE(RQ.t_DealPart, 1, 0, 2)
                              AND Leg.t_PFI = case when pFIID != -1 then pFIID else Leg.t_PFI end
                              AND RQ.t_FactDate >= pBegDate
                              AND RQ.t_FactDate <= pEndDate
                              AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
                              AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                              AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                              AND (RQ.t_DealPart = 1 OR
                                   (RQ.t_DealPart = 2 AND
                                    ((Rsb_Secur.IsBackSale(oGrp)=1 OR RQ.t_DocKind = RSI_NPTXC.DL_CONVAVR
                                     ) or
                                     ((Rsb_Secur.IsRepo(Opr.oGrp)=1 OR Rsb_Secur.IsLoan(Opr.oGrp)=1
                                      ) and
                                      --категория "Является налоговым Репо" на сделке DDL_TICK.T_DEALID задана и равна False
                                      ( CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(Tick.t_DealID, 34, '0'), 2)=1
                                      )
                                     )
                                    )
                                   )
                                  )
                              AND Tick.t_ClientID = pClient
                              AND RSI_NPTO.CheckContrIIS(Tick.t_ClientContrID)  = pIIS
                              AND ((pContract IS NULL) OR (pContract <= 0) OR (Tick.t_CLientContrID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                              AND (   RQ.t_DocKind IN ( RSI_NPTXC.DL_SECURITYDOC, RSI_NPTXC.DL_RETIREMENT )
                                   OR (RQ.t_DocKind = RSI_NPTXC.DL_CONVAVR AND Tick.t_DealDate < TO_DATE('01.01.2015','DD.MM.YYYY')) 
                                  )
                              AND Rsb_Secur.IsRet_Partly(Opr.oGrp) <> 1
                              AND Rsb_Secur.IsRet_Coupon(Opr.oGrp) <> 1
                              AND not (RSI_RSB_FIINSTR.FI_ISKSU (tick.t_pfi) = 1 and (RSB_SECUR.DealIsRepo(tick.t_dealid)=1 or RSB_SECUR.IsAvrWrtIn(Opr.oGrp)=1 or RSB_SECUR.IsAvrWrtOut(Opr.oGrp)=1));


       --Занести лоты покупок/продаж для клиента-контрагента, совершенных за период.
       INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                  T_DOCID          ,
                                  T_RQID           ,
                                  T_DEALDATE       ,
                                  T_DEALTIME       ,
                                  T_DEALCODE       ,
                                  T_DEALCODETS     ,
                                  T_CLIENT         ,
                                  T_CONTRACT       ,
                                  T_FIID           ,
                                  T_KIND           ,
                                  T_AMOUNT         ,
                                  T_PRICE          ,
                                  T_PRICEFIID      ,
                                  T_TOTALCOST      ,
                                  T_NKD            ,
                                  T_BUYDATE        ,
                                  T_SALEDATE       ,
                                  T_BLOCKED        ,
                                  T_NOTCOUNTEDONIIS)
                           SELECT /*+ index(Tick DDL_TICK_DBT_IDX_U2)*/
                                  RQ.t_DocKind,                                                              --T_DOCKIND    
                                  RQ.t_DocID,                                                                --T_DOCID      
                                  RQ.t_ID,                                                                   --T_RQID  
                                  Tick.t_DealDate,                                                           --T_DEALDATE
                                  Tick.t_DealTime,                                                           --T_DEALTIME
                                  RTRIM(Tick.t_DealCode),                                                    --T_DEALCODE
                                  RTRIM(DECODE(Tick.t_DealCodeTS,CHR(1),Tick.t_DealCode,Tick.t_DealCodeTS)), --T_DEALCODETS
                                  pClient,                                                                   --T_CLIENT     
                                  Tick.t_PartyContrID,                                                       --T_CONTRACT   
                                  Leg.t_PFI,                                                                 --T_FIID       
                                  get_lotKind(Opr.oGrp, Tick.t_DealID, RQ.t_DealPart, 1),                    --T_KIND       
                                  RQ.t_Amount,                                                               --T_AMOUNT     
                                  (CASE WHEN Leg.t_RelativePrice = CHR(0) OR
                                             Leg.t_RelativePrice IS NULL OR
                                             RQ.t_DocKind = RSI_NPTXC.DL_RETIREMENT THEN Leg.t_Price
                                        ELSE RSI_RSB_FIInstr.FI_GetNominalOnDate(Leg.t_PFI, 
                                             RQ.t_FactDate)*Leg.t_Price/100 END),                            --T_PRICE
                                  Leg.t_CFI,                                                                 --T_PRICEFIID
                                  Leg.t_TotalCost*RQ.t_Amount/Leg.t_Principal,                               --T_TOTALCOST  
                                  Leg.t_NKD*RQ.t_Amount/Leg.t_Principal,                                     --T_NKD        
                                  get_lotBuyDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart, 1),  --T_BUYDATE       
                                  get_lotSaleDate(Opr.oGrp, Tick.t_DealID, RQ.t_FactDate, RQ.t_DealPart, 1), --T_SALEDATE       
                                  Tick.T_BLOCKED, --T_BLOCKED
                                  RSI_NPTX.DetermineIISCountingStatus(RQ.t_DocKind, RQ.t_DocID, Tick.t_ClientContrID) --T_NOTCOUNTEDONIIS
                             FROM ddlrq_dbt RQ, ddl_tick_dbt Tick, ddl_leg_dbt Leg,
                                  (SELECT t_Kind_Operation, t_DocKind, rsb_secur.get_OperationGroup(t_SysTypes) oGrp,
                                          Rsb_Secur.IsBackSale(rsb_secur.get_OperationGroup(t_SysTypes)) as IsBackSale,
                                          Rsb_Secur.IsRepo(rsb_secur.get_OperationGroup(t_SysTypes)) as IsRepo,
                                          RSB_SECUR.IsAvrWrtIn(rsb_secur.get_OperationGroup(t_SysTypes)) as IsAvrWrtIn,
                                          RSB_SECUR.IsAvrWrtOut(rsb_secur.get_OperationGroup(t_SysTypes)) as IsAvrWrtOut,
                                          RSB_SECUR.IsLoan(rsb_secur.get_OperationGroup(t_SysTypes)) as IsLoan
                                     FROM doprkoper_dbt
                                    WHERE t_DocKind = RSI_NPTXC.DL_SECURITYDOC
                                  ) Opr
                            WHERE Tick.t_BOfficeKind = RSI_NPTXC.DL_SECURITYDOC
                              AND Tick.t_DealType = Opr.t_Kind_Operation
                              AND Tick.t_PartyID = pClient
                              AND Tick.t_IsPartyClient = 'X'
                              AND Tick.t_DealDate <= pEndDate
                              AND RSI_NPTO.CheckContrIIS(Tick.t_PartyContrID) = pIIS
                              AND ((pContract IS NULL) OR (pContract <= 0) OR (Tick.t_PartyContrID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                              AND Leg.t_DealID = Tick.t_DealID
                              AND Leg.t_LegID = 0
                              AND Leg.t_LegKind = DECODE(RQ.t_DealPart, 1, 0, 2)
                              AND Leg.t_PFI = case when pFIID != -1 then pFIID else Leg.t_PFI end
                              AND RQ.t_DocKind = Tick.t_BOfficeKind
                              AND RQ.t_DocID = Tick.t_DealID 
                              AND RQ.t_FactDate >= pBegDate
                              AND RQ.t_FactDate <= pEndDate
                              AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
                              AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                              AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                              AND (RQ.t_DealPart = 1 OR
                                   (RQ.t_DealPart = 2 AND
                                    ((Opr.IsBackSale=1 OR RQ.t_DocKind = RSI_NPTXC.DL_CONVAVR
                                     ) or
                                     ((Opr.IsRepo=1 OR Opr.IsLoan=1
                                      ) and
                                      --категория "Является налоговым Репо" на сделке DDL_TICK.T_DEALID задана и равна False
                                      ( CheckCateg(RSB_Secur.OBJTYPE_SECDEAL, 23, LPAD(Tick.t_DealID, 34, '0'), 2)=1
                                      )
                                     )
                                    )
                                   )
                                  )
                              AND not (RSI_RSB_FIINSTR.FI_ISKSU (tick.t_pfi) = 1 and (Opr.IsRepo=1 or Opr.IsAvrWrtIn=1 or Opr.IsAvrWrtOut=1));

       --Занести лоты зачислений/списаний, совершенных за период.
       INSERT INTO dnptxlot_dbt ( T_DOCKIND        ,
                                  T_DOCID          ,
                                  T_RQID           ,
                                  T_DEALDATE       ,
                                  T_DEALTIME       ,
                                  T_DEALCODE       ,
                                  T_DEALCODETS     ,
                                  T_CLIENT         ,
                                  T_CONTRACT       ,
                                  T_FIID           ,
                                  T_KIND           ,
                                  T_AMOUNT         ,
                                  T_PRICE          ,
                                  T_PRICEFIID      ,
                                  T_TOTALCOST      ,
                                  T_NKD            ,
                                  T_BUYDATE        ,
                                  T_SALEDATE       ,
                                  T_NOTCOUNTEDONIIS)
                           SELECT RQ.t_DocKind,                                                              --T_DOCKIND    
                                  RQ.t_DocID,                                                                --T_DOCID      
                                  RQ.t_ID,                                                                   --T_RQID  
                                  Tick.t_DealDate,                                                           --T_DEALDATE
                                  (CASE WHEN (Rsb_Secur.IsAvrWrtIn(Opr.oGrp)=1 
                                          and Leg.t_SupplyTime is not null 
                                          and Leg.t_SupplyTime <> TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS')
                                             )
                                        THEN Leg.t_SupplyTime
                                        ELSE Tick.t_DealTime END),                                           --T_DEALTIME
                                  RTRIM(Tick.t_DealCode),                                                    --T_DEALCODE
                                  RTRIM(DECODE(Tick.t_DealCodeTS,CHR(1),Tick.t_DealCode,Tick.t_DealCodeTS)), --T_DEALCODETS
                                  pClient,                                                                   --T_CLIENT     
                                  Tick.t_ClientContrID,                                                      --T_CONTRACT   
                                  Leg.t_PFI,                                                                 --T_FIID       
                                  (CASE WHEN Rsb_Secur.IsAvrWrtIn(Opr.oGrp)=1
                                        THEN RSI_NPTXC.NPTXLOTS_BUY
                                        ELSE RSI_NPTXC.NPTXLOTS_SALE END),                                   --T_KIND       
                                  RQ.t_Amount,                                                               --T_AMOUNT     
                                  (CASE WHEN Rsb_Secur.IsAvrWrtIn(Opr.oGrp)=1
                                        THEN RSI_NPTO.GetPriceFromAvrWrtIn(Tick.t_DealID, Leg.t_Price)
                                        ELSE Leg.t_Price END),                                               --T_PRICE       
                                  (CASE WHEN Rsb_Secur.IsAvrWrtIn(Opr.oGrp)=1
                                        THEN RSI_NPTO.GetPriceFIIDFromAvrWrtIn(Tick.t_DealID, Leg.t_CFI)
                                        ELSE Leg.t_CFI END),                                                 --T_PRICEFIID
                                  (CASE WHEN Rsb_Secur.IsAvrWrtIn(Opr.oGrp)=1
                                        THEN RSI_NPTO.GetCostFromAvrWrtIn(Tick.t_DealID, Leg.t_TotalCost - RSI_RSB_FIInstr.ConvSum(Leg.t_NKD, Leg.t_NKDFIID, Leg.t_CFI, NVL(RQ.t_FactDate, Tick.t_DealDate), 1))
                                        ELSE Leg.t_TotalCost - RSI_RSB_FIInstr.ConvSum(Leg.t_NKD, Leg.t_NKDFIID, Leg.t_CFI, NVL(RQ.t_FactDate, Tick.t_DealDate), 1) END) + 
                                  (CASE WHEN Rsb_Secur.IsAvrWrtIn(Opr.oGrp)=1
                                        THEN RSI_RSB_FIInstr.ConvSum(RSI_NPTO.GetNkdFromAvrWrtIn(Tick.t_DealID, Leg.t_NKD), Leg.t_NKDFIID, Leg.t_CFI, NVL(RQ.t_FactDate, Tick.t_DealDate), 1)
                                        ELSE RSI_RSB_FIInstr.ConvSum(Leg.t_NKD, Leg.t_NKDFIID, Leg.t_CFI, NVL(RQ.t_FactDate, Tick.t_DealDate), 1) END), --T_TOTALCOST
                                  (CASE WHEN Rsb_Secur.IsAvrWrtIn(Opr.oGrp)=1
                                        THEN RSI_RSB_FIInstr.ConvSum(RSI_NPTO.GetNkdFromAvrWrtIn(Tick.t_DealID, Leg.t_NKD), Leg.t_NKDFIID, Leg.t_CFI, NVL(RQ.t_FactDate, Tick.t_DealDate), 1)
                                        ELSE RSI_RSB_FIInstr.ConvSum(Leg.t_NKD, Leg.t_NKDFIID, Leg.t_CFI, NVL(RQ.t_FactDate, Tick.t_DealDate), 1) END), --T_NKD
                                  (CASE WHEN Rsb_Secur.IsAvrWrtIn(Opr.oGrp)=1
                                        THEN RSI_NPTO.GetDateFromAvrWrtIn(Tick.t_DealID, RQ.t_FactDate, Tick.t_DealDate)
                                        ELSE TO_DATE('01.01.0001','DD.MM.YYYY') END),                        --T_BUYDATE
                                  (CASE WHEN Rsb_Secur.IsAvrWrtOut(Opr.oGrp)=1
                                        THEN RQ.t_FactDate
                                        ELSE TO_DATE('01.01.0001','DD.MM.YYYY') END),                        --T_SALEDATE
                                  RSI_NPTX.DetermineIISCountingStatus(RQ.t_DocKind, RQ.t_DocID, Tick.t_ClientContrID) --T_NOTCOUNTEDONIIS
                             FROM ddlrq_dbt RQ, ddl_tick_dbt Tick, ddl_leg_dbt Leg,
                                  (SELECT t_Kind_Operation, t_DocKind, rsb_secur.get_OperationGroup(t_SysTypes) oGrp
                                     FROM doprkoper_dbt) Opr
                            WHERE Tick.t_DealID = RQ.t_DocID
                              AND Tick.t_BOfficeKind = RQ.t_DocKind
                              AND Opr.t_Kind_Operation = Tick.t_DealType
                              AND Opr.t_DocKind = Tick.t_BOfficeKind
                              AND Leg.t_DealID = Tick.t_DealID
                              AND Leg.t_LegID = 0
                              AND Leg.t_LegKind = 0
                              AND Leg.t_PFI = case when pFIID != -1 then pFIID else Leg.t_PFI end
                              AND RQ.t_FactDate >= pBegDate
                              AND RQ.t_FactDate <= pEndDate
                              AND RQ.t_State   = RSI_DLRQ.DLRQ_STATE_EXEC
                              AND RQ.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
                              AND RQ.t_Type    = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                              AND RQ.t_DealPart = 1
                              AND Tick.t_ClientID = pClient
                              AND RSI_NPTO.CheckContrIIS(Tick.t_ClientContrID)  = pIIS  
                              AND ((pContract IS NULL) OR (pContract <= 0) OR (Tick.t_ClientContrID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                              AND Tick.t_Flag3 = CHR(88)
                              AND RQ.t_DocKind = RSI_NPTXC.DL_AVRWRT
                              AND not (RSI_RSB_FIINSTR.FI_ISKSU (tick.t_pfi) = 1 and (RSB_SECUR.DealIsRepo(tick.t_dealid)=1 or RSB_SECUR.IsAvrWrtIn(Opr.oGrp)=1 or RSB_SECUR.IsAvrWrtOut(Opr.oGrp)=1));

       --Установить дату лотов по 2 части Репо и займа, совершенных за период
       FOR c IN BackPaym LOOP

          IF( c.t_Kind IN ( RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT ) ) THEN

            UPDATE dnptxlot_dbt TXLot
               SET TXLot.t_BuyDate = c.t_FactDate
             WHERE TXLot.t_BegLotID = c.t_BegLotID
               AND TXLot.t_BuyDate = TO_DATE('01.01.0001','DD.MM.YYYY');

            UPDATE dnptxlot_dbt TXLot
               SET TXLot.t_BegBuyDate = c.t_FactDate
             WHERE TXLot.t_BegLotID = c.t_BegLotID
               AND TXLot.t_BegBuyDate = TO_DATE('01.01.0001','DD.MM.YYYY');

          ELSE

            UPDATE dnptxlot_dbt TXLot
               SET TXLot.t_SaleDate = c.t_FactDate
             WHERE TXLot.t_BegLotID = c.t_BegLotID
               AND TXLot.t_SaleDate = TO_DATE('01.01.0001','DD.MM.YYYY');

            UPDATE dnptxlot_dbt TXLot
               SET TXLot.t_BegSaleDate = c.t_FactDate
             WHERE TXLot.t_BegLotID = c.t_BegLotID
               AND TXLot.t_BegSaleDate = TO_DATE('01.01.0001','DD.MM.YYYY');

          END IF;
       END LOOP;

       --Обработать ТО по компенсационной поставке по Репо и займам за период.
       --Перебираем суммарные комп. ТО за день, сгруппированные по сделке, бумаге, направлению.
       FOR cpaym IN comppaym
       LOOP
          v_Kind := -1;
          IF cpaym.SumAmount > 0 THEN
             v_Kind := rsi_dlrq.dlrq_kind_commit;
          ELSIF cpaym.SumAmount < 0 THEN
             v_Kind := rsi_dlrq.dlrq_kind_request;
          END IF;

          IF v_Kind > -1 THEN
             -- Найти ТО с максимальным количеством бумаг направления v_Kind:
             BEGIN
               SELECT * INTO v_CompRQ
                 FROM (SELECT rq.* 
                         FROM ddlrq_dbt rq
                        WHERE rq.t_dockind = RSI_NPTXC.DL_SECURITYDOC
                          AND rq.t_docid = cpaym.t_docid
                          AND rq.t_fiid = cpaym.t_fiid
                          AND rq.t_state = rsi_dlrq.dlrq_state_exec
                          AND rq.t_subkind = rsi_dlrq.dlrq_subkind_avoiriss
                          AND rq.t_kind = v_Kind
                          AND rq.t_type = rsi_dlrq.dlrq_type_compdelivery
                          AND rq.t_factdate = cpaym.t_factdate
                        ORDER BY rq.t_Amount DESC, rq.t_ID ASC)
                WHERE ROWNUM = 1;
             EXCEPTION
                WHEN NO_DATA_FOUND THEN EXIT;
                WHEN OTHERS THEN RETURN;
             END;

             BEGIN
                -- получить сделку по ТО.
                SELECT tick.t_dealcode, tick.t_DealCodeTS, tick.t_dealdate, tick.t_dealtime, tick.t_ClientContrID,
                       NVL(leg2.t_price, 0), NVL(leg2.t_TotalCost, 0), NVL(leg2.t_NKD, 0), leg2.t_cfi,
                       rsb_secur.issale (opr.ogrp), rsb_secur.isbuy (opr.ogrp), opr.ogrp
                  INTO v_dealcode, v_dealcodets, v_dealdate, v_dealtime, v_ClientContrID,
                       v_legprice2, v_legTotalCost2, v_legNKD2, v_legcfi2,
                       issale, isbuy, ogrp
                  FROM ddl_leg_dbt leg1,
                       ddl_leg_dbt leg2,
                       ddl_tick_dbt tick,
                       (SELECT t_kind_operation, t_dockind,
                               rsb_secur.get_operationgroup (t_systypes) ogrp
                          FROM doprkoper_dbt) opr
                 WHERE opr.t_kind_operation = tick.t_dealtype
                   AND opr.t_dockind = tick.t_bofficekind
                   AND tick.t_dealid = cpaym.t_docid
                   AND leg1.t_dealid = cpaym.t_docid
                   AND leg1.t_legkind = 0
                   AND leg2.t_dealid(+) = cpaym.t_docid
                   AND leg2.t_legkind(+) = 2;
             EXCEPTION
                WHEN NO_DATA_FOUND
                THEN
                   BEGIN
                      RSI_NPTMSG.PutMsg(RSI_NPTXC.MES_WARN, 'Ошибка обработки компенсационной поставки по сделке c DealID = '||cpaym.t_docid
                                                            || ' в дату '
                                                            || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY'));
                   END;
             END;

             if cpaym.SumAmount < 0 THEN 
                v_rqamount := -cpaym.SumAmount;
             else
                v_rqamount := cpaym.SumAmount;
             end if;
      
             -- в прямом РЕПО - требования, в обратном - обязательства. Уменьшение обеспечения
             IF (   (v_Kind = 0 AND issale = 1)
                 OR (v_Kind = 1 AND isbuy = 1)
                )
             THEN
                v_restpaymamount := v_rqamount;
                                       -- необработанное количество комп. взноса.
                --Для каждого комп. ТО, обработать все лоты соотв. РЕПО. Сортировка по убыванию.
                v_break := FALSE;
      
                WHILE v_restpaymamount > 0 AND v_break = FALSE
                LOOP
                   IF rsb_secur.isbuy (ogrp) = 1
                   THEN
                      BEGIN
                         SELECT *
                           INTO clot
                           FROM (SELECT   *
                                     FROM dnptxlot_dbt txlot
                                    WHERE txlot.t_fiid = cpaym.t_fiid
                                      AND txlot.t_DocKind = RSI_NPTXC.DL_SECURITYDOC
                                      AND txlot.t_docid = cpaym.t_docid
                                      AND txlot.t_Kind IN
                                             (RSI_NPTXC.NPTXLOTS_BACKREPO,
                                              RSI_NPTXC.NPTXLOTS_LOANGET
                                             )
                                      AND txlot.t_childid = 0
                                      AND (   txlot.t_saledate >= cpaym.t_factdate
                                           OR txlot.t_saledate =
                                                 TO_DATE ('01.01.0001',
                                                          'DD.MM.YYYY'
                                                         )
                                          )
                                 ORDER BY txlot.t_begbuydate DESC,
                                          txlot.t_dealdate DESC,
                                          txlot.t_dealtime DESC,
                                          NLSSORT(txlot.T_SORTCODE) DESC)
                          WHERE ROWNUM = 1;
                      EXCEPTION
                         WHEN NO_DATA_FOUND
                         THEN
                            BEGIN
                               RSI_NPTMSG.PutMsg(RSI_NPTXC.MES_WARN, 'Ошибка обработки компенсационной поставки по сделке "'
                                                                     || v_dealcode
                                                                     || '" в дату '
                                                                     || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY'));
                               v_break := TRUE;
                            END;
                      END;
                   ELSE
                      BEGIN
                         SELECT *
                           INTO clot
                           FROM (SELECT   *
                                     FROM dnptxlot_dbt txlot
                                    WHERE txlot.t_fiid = cpaym.t_fiid
                                      AND txlot.t_DocKind = RSI_NPTXC.DL_SECURITYDOC
                                      AND txlot.t_docid = cpaym.t_docid
                                      AND txlot.t_Kind IN
                                             (RSI_NPTXC.NPTXLOTS_LOANPUT,
                                              RSI_NPTXC.NPTXLOTS_REPO
                                             )
                                      AND txlot.t_childid = 0
                                      AND (   txlot.t_buydate >= cpaym.t_factdate
                                           OR txlot.t_buydate =
                                                 TO_DATE ('01.01.0001',
                                                          'DD.MM.YYYY'
                                                         )
                                          )
                                 ORDER BY txlot.t_begsaledate DESC,
                                          txlot.t_dealdate DESC,
                                          txlot.t_dealtime DESC,
                                          NLSSORT(txlot.T_SORTCODE) DESC)
                          WHERE ROWNUM = 1;
                      EXCEPTION
                         WHEN NO_DATA_FOUND
                         THEN
                            BEGIN
                               RSI_NPTMSG.PutMsg(RSI_NPTXC.MES_WARN, 'Ошибка обработки компенсационной поставки по сделке "'
                                                                     || v_dealcode
                                                                     || '" в дату '
                                                                     || TO_CHAR (cpaym.t_factdate, 'DD.MM.YYYY'));
                               v_break := TRUE;
                            END;
                      END;
                   END IF;
      
                   EXIT WHEN v_break;
      
                   -- Если лот скомпенсирован полностью, то установить в нём дату 2ч. На связи (для ПР) и на 2ч (для ОР) признак возврата будет выставлен в createlots. Комп. лот не создавать.
                   IF v_restpaymamount >= clot.t_amount
                   THEN
                      IF clot.t_Kind IN
                            (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET)
                      THEN
                         UPDATE dnptxlot_dbt txlot
                            SET txlot.t_childid = -1,
                                txlot.t_saledate = cpaym.t_factdate,
                                txlot.t_begsaledate = cpaym.t_factdate
                          WHERE txlot.t_id = clot.t_id;
                      ELSE
                         UPDATE dnptxlot_dbt txlot
                            SET txlot.t_childid = -1,
                                txlot.t_buydate = cpaym.t_factdate,
                                txlot.t_begbuydate = cpaym.t_factdate
                          WHERE txlot.t_id = clot.t_id;
                      END IF;
      
                      v_restpaymamount := v_restpaymamount - clot.t_amount;
                   ELSE
                      -- Если лот скомпенсирован не полностью, то оставить старую обработку для него. Создать комп. лот и т.д.
                      v_amount := clot.t_amount;
                      v_buydate := clot.t_buydate;
                      v_saledate := clot.t_saledate;

                      v_buydate :=
                         iif (clot.t_Kind IN
                                 (RSI_NPTXC.NPTXLOTS_BACKREPO,
                                  RSI_NPTXC.NPTXLOTS_LOANGET
                                 ),
                              cpaym.t_factdate,
                              v_buydate
                             );
                      v_saledate :=
                         iif (clot.t_Kind IN
                                 (RSI_NPTXC.NPTXLOTS_BACKREPO,
                                  RSI_NPTXC.NPTXLOTS_LOANGET
                                 ),
                              v_saledate,
                              cpaym.t_factdate
                             );
                      v_begbuydate :=
                         iif (clot.t_Kind IN
                                 (RSI_NPTXC.NPTXLOTS_BACKREPO,
                                  RSI_NPTXC.NPTXLOTS_LOANGET
                                 ),
                              clot.t_begbuydate,
                              v_buydate
                             );
                      v_begsaledate :=
                         iif (clot.t_Kind IN
                                 (RSI_NPTXC.NPTXLOTS_BACKREPO,
                                  RSI_NPTXC.NPTXLOTS_LOANGET
                                 ),
                              v_saledate,
                              clot.t_begsaledate
                             );
                      --можем скомпенсировать на этом лоте: нескомпенсированный остаток v_RestPaymAmount
                      v_amount := v_amount - v_restpaymamount;

                      SELECT Count(1) INTO v_Count
                        FROM DNPTXLOT_DBT
                       WHERE T_DocID = clot.T_DocID
                         AND T_DocKind = clot.T_DocKind
                         AND t_Client = pClient
                         AND RSI_NPTO.CheckContrIIS(t_Contract) = pIIS
                         AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)));

                      INSERT INTO dnptxlot_dbt ( T_DOCKIND,
                                                 T_DOCID,
                                                 T_RQID,
                                                 T_DEALDATE,
                                                 T_DEALTIME,
                                                 T_DEALCODE,
                                                 T_DEALCODETS,
                                                 T_CLIENT,
                                                 T_CONTRACT,
                                                 T_FIID,
                                                 T_KIND,
                                                 T_TYPE,
                                                 T_BEGLOTID,
                                                 T_CHILDID,
                                                 T_AMOUNT,
                                                 T_COMPAMOUNT,
                                                 T_PRICE,
                                                 T_PRICEFIID,
                                                 T_TOTALCOST,
                                                 T_NKD,
                                                 T_BUYDATE,
                                                 T_SALEDATE,
                                                 T_BEGBUYDATE,
                                                 T_BEGSALEDATE,
                                                 T_SORTCODE,
                                                 T_INACC,
                                                 T_NOTCOUNTEDONIIS)
                                         VALUES( clot.t_DocKind,                              --T_DOCKIND    
                                                 clot.t_DocID,                                --T_DOCID
                                                 v_CompRQ.t_id,                               --T_RQID  
                                                 clot.t_DealDate,                             --T_DEALDATE
                                                 clot.t_DealTime,                             --T_DEALTIME
                                                 get_lotCode(RTRIM(clot.t_DealCode), v_Count),--T_DEALCODE
                                                 get_lotCode(RTRIM(DECODE(clot.t_DealCodeTS,CHR(1),clot.t_DealCode,clot.t_DealCodeTS)), v_Count),   --T_DEALCODETS
                                                 clot.t_Client,                               --T_CLIENT     
                                                 clot.t_Contract,                             --T_CONTRACT   
                                                 clot.t_FIID,                                 --T_FIID       
                                                 clot.t_Kind,                                 --T_KIND
                                                 RSI_NPTXC.NPTXDEAL_COMP,                     --T_TYPE       
                                                 clot.t_BegLotID,                             --T_BEGLOTID
                                                 0,                                           --T_CHILDID 
                                                 v_Amount,                                    --T_AMOUNT     
                                                 v_RestPaymAmount,                            --T_COMPAMOUNT
                                                 v_legprice2,                                 --T_PRICE    
                                                 clot.t_PriceFIID,                            --T_PRICEFIID
                                                 v_legTotalCost2,                             --T_TOTALCOST
                                                 v_legNKD2,                                   --T_NKD
                                                 v_BuyDate,                                   --T_BUYDATE    
                                                 v_SaleDate,                                  --T_SALEDATE   
                                                 v_BegBuyDate,                                --T_BEGBUYDATE 
                                                 v_BegSaleDate,                               --T_BEGSALEDATE
                                                 clot.t_SortCode,                             --T_SORTCODE
                                                 CHR(0),                                      --T_INACC
                                                 RSI_NPTX.DetermineIISCountingStatus(clot.t_DocKind, clot.t_DocID, clot.t_Contract) --T_NOTCOUNTEDONIIS
                                               ) RETURNING t_ID INTO v_lotid;

                      UPDATE dnptxlot_dbt
                         SET t_childid = v_lotid
                       WHERE t_id = clot.t_id;

                      v_restpaymamount := 0;
                   END IF;
                END LOOP;
             ELSE
                --Создать лоты комп. поставки по увеличению обеспечения в ПР и ОР.

                SELECT Count(1) INTO v_Count
                  FROM DNPTXLOT_DBT
                 WHERE T_DocID = v_CompRQ.t_DocID
                   AND T_DocKind = v_CompRQ.t_DocKind
                   AND t_Client = pClient
                   AND RSI_NPTO.CheckContrIIS(t_Contract) = pIIS
                   AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)));

                INSERT INTO dnptxlot_dbt ( T_DOCKIND,
                                           T_DOCID,
                                           T_RQID,
                                           T_DEALDATE,
                                           T_DEALTIME,
                                           T_DEALCODE,
                                           T_DEALCODETS,
                                           T_CLIENT,
                                           T_CONTRACT,
                                           T_FIID,
                                           T_KIND,
                                           T_TYPE,
                                           T_BEGLOTID,
                                           T_CHILDID,
                                           T_AMOUNT,
                                           T_COMPAMOUNT,
                                           T_PRICE,
                                           T_PRICEFIID,
                                           T_TOTALCOST,
                                           T_NKD,
                                           T_BUYDATE,
                                           T_SALEDATE,
                                           T_BEGBUYDATE,
                                           T_BEGSALEDATE,
                                           T_SORTCODE,
                                           T_INACC,
                                           T_NOTCOUNTEDONIIS)
                                   VALUES( v_CompRQ.t_DocKind,                          --T_DOCKIND
                                           v_CompRQ.t_DocID,                            --T_DOCID
                                           v_CompRQ.t_id,                               --T_RQID
                                           v_dealdate,                                  --T_DEALDATE
                                           v_dealtime,                                  --T_DEALTIME
                                           get_lotCode(RTRIM(v_dealcode), v_Count),     --T_DEALCODE
                                           get_lotCode(RTRIM(DECODE(v_dealcodets,CHR(1),v_dealcode,v_dealcodets)), v_Count),   --T_DEALCODETS
                                           pClient,                                     --T_CLIENT
                                           v_ClientContrID,                             --T_CONTRACT   
                                           v_CompRQ.t_FIID,                             --T_FIID       
                                           (CASE
                                               WHEN isbuy = 1
                                                  THEN RSI_NPTXC.NPTXLOTS_BACKREPO
                                               ELSE RSI_NPTXC.NPTXLOTS_REPO
                                            END
                                           ),                                           --T_KIND
                                           RSI_NPTXC.NPTXDEAL_REAL,                     --T_TYPE       
                                           0,                                           --T_BEGLOTID
                                           0,                                           --T_CHILDID 
                                           v_rqamount,                                  --T_AMOUNT     
                                           0,                                           --T_COMPAMOUNT
                                           v_legprice2,                                 --T_PRICE    
                                           v_legcfi2,                                   --T_PRICEFIID
                                           v_legTotalCost2,                             --T_TOTALCOST
                                           v_legNKD2,                                   --T_NKD
                                           (CASE
                                               WHEN isbuy = 1
                                                  THEN v_CompRQ.t_factdate
                                               ELSE TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                                            END
                                           ),                                           --T_BUYDATE    
                                           (CASE
                                               WHEN isbuy = 1
                                                  THEN TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                                               ELSE v_CompRQ.t_factdate
                                            END
                                           ),                                           --T_SALEDATE   
                                           TO_DATE ('01.01.0001', 'DD.MM.YYYY'),        --T_BEGBUYDATE 
                                           TO_DATE ('01.01.0001', 'DD.MM.YYYY'),        --T_BEGSALEDATE
                                           CHR(1),                                      --T_SORTCODE
                                           CHR(88),                                     --T_INACC
                                           RSI_NPTX.DetermineIISCountingStatus(v_CompRQ.t_DocKind, v_CompRQ.t_DocID, v_ClientContrID) --T_NOTCOUNTEDONIIS
                                         ) RETURNING t_ID INTO v_lotid;

                UPDATE dnptxlot_dbt
                   SET t_beglotid = t_id
                 WHERE t_id = v_lotid;
      
                --Установить дату по 2 части
                SELECT NVL (MIN (rq.t_factdate),
                            TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                           )
                  INTO v_date2
                  FROM ddlrq_dbt rq
                 WHERE rq.t_docid = cpaym.t_docid
                   AND rq.t_dockind = RSI_NPTXC.DL_SECURITYDOC
                   AND rq.t_state = rsi_dlrq.dlrq_state_exec
                   AND rq.t_subkind = rsi_dlrq.dlrq_subkind_avoiriss
                   AND rq.t_type = rsi_dlrq.dlrq_type_delivery
                   AND rq.t_dealpart = 2
                   AND rq.t_factdate >= pBegDate
                   AND rq.t_factdate <= pEndDate;

                IF v_date2 <> TO_DATE ('01.01.0001', 'DD.MM.YYYY')
                THEN
                   IF (isbuy = 0)
                   THEN
                      UPDATE dnptxlot_dbt txlot
                         SET txlot.t_buydate = v_date2,
                             txlot.t_begbuydate = v_date2
                       WHERE txlot.t_id = v_lotid;
                   ELSE
                      UPDATE dnptxlot_dbt txlot
                         SET txlot.t_saledate = v_date2,
                             txlot.t_begsaledate = v_date2
                       WHERE txlot.t_id = v_lotid;
                   END IF;
                END IF;
             END IF;
          END IF;
       END LOOP;

    END;

    --Корректирует существующую запись таблицы состояния
    --Возвращает 0, если запись успешно исправлена, 1 если не найдена
    --Возвращает True, если запись успешно исправлена, False  если не найдена
    FUNCTION CorrectExistTS (pType IN NUMBER, pBuyID IN NUMBER, pSaleID IN NUMBER, 
                        pDate IN DATE, pAmount IN NUMBER)
      RETURN BOOLEAN
    IS
      v_T      DNPTXTS_DBT%ROWTYPE;
      v_Amount NUMBER;
    BEGIN

      BEGIN
        SELECT *
          INTO v_T
          FROM dnptxts_dbt
         WHERE t_Type   = pType
           AND t_BuyID  = pBuyID
           AND t_SaleID = pSaleID
           AND (t_EndDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_EndDate > pDate);

        IF v_T.t_BegDate > pDate THEN
           RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Некорректная дата начала записи в таблице состояний' );
           RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20620,''); 
        END IF;


        RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'v_T.t_Amount = '||v_T.t_Amount||', pAmount = '||pAmount );
        v_Amount := v_T.t_Amount + pAmount;
        IF v_Amount < 0 THEN
           RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Отрицательный остаток в записи в таблице состояний' );
           RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20621,''); 
        END IF;

        IF v_T.t_BegDate = pDate THEN
          IF v_Amount = 0 THEN
             DELETE 
               FROM DNPTXTS_DBT 
              WHERE t_ID = v_T.t_ID;
          ELSE
             UPDATE dnptxts_dbt
                SET t_Amount = v_Amount
              WHERE t_ID = v_T.t_ID;
          END IF;
        ELSE
          UPDATE dnptxts_dbt
             SET t_EndDate = pDate
           WHERE t_ID = v_T.t_ID;

          IF v_Amount > 0 THEN
             INSERT INTO dnptxts_dbt( T_CLIENT   ,
                                      T_CONTRACT ,
                                      T_FIID     ,
                                      T_BUYID    ,
                                      T_SALEID   ,
                                      T_TYPE     ,
                                      T_BEGDATE  ,
                                      T_ENDDATE  ,
                                      T_AMOUNT  
                                    )
                              VALUES(
                                      v_T.t_Client,                        --T_CLIENT  
                                      v_T.t_Contract,                      --T_CONTRACT
                                      v_T.t_FIID,                          --T_FIID    
                                      v_T.t_BuyID,                         --T_BUYID   
                                      v_T.t_SaleID,                        --T_SALEID  
                                      v_T.t_Type,                          --T_TYPE    
                                      pDate,                               --T_BEGDATE 
                                      TO_DATE('01.01.0001', 'DD.MM.YYYY'), --T_ENDDATE 
                                      v_Amount                             --T_AMOUNT  
                                    );
          END IF;
        END IF;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          BEGIN
            RETURN FALSE; -- False
          END;
      END;

      RETURN TRUE;
    END; --CorrectExistTS

    --Вставляет запись таблицы состояния
    --Не должна использоваться внутри триггеров по лотам.
    PROCEDURE CreateTS (pType IN NUMBER, pBuyID IN NUMBER, pSaleID IN NUMBER, 
                       pDate IN DATE, pAmount IN NUMBER)
    IS
      v_Lot    DNPTXLOT_DBT%ROWTYPE;
    BEGIN

      IF pAmount <= 0 THEN
         RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Остаток в записи таблицы состояний должен быть положительным' );
         RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20627,''); 
      END IF;

      BEGIN
        SELECT *
          INTO v_Lot
          FROM dnptxlot_dbt
         WHERE t_ID = pBuyID;

        INSERT INTO dnptxts_dbt( T_CLIENT   ,
                                 T_CONTRACT ,
                                 T_FIID     ,
                                 T_BUYID    ,
                                 T_SALEID   ,
                                 T_TYPE     ,
                                 T_BEGDATE  ,
                                 T_ENDDATE  ,
                                 T_AMOUNT  
                               )
                         VALUES(
                                 v_Lot.t_Client,                      --T_CLIENT  
                                 v_Lot.t_Contract,                    --T_CONTRACT
                                 v_Lot.t_FIID,                        --T_FIID    
                                 pBuyID,                              --T_BUYID   
                                 pSaleID,                             --T_SALEID  
                                 pType,                               --T_TYPE    
                                 pDate,                               --T_BEGDATE 
                                 TO_DATE('01.01.0001', 'DD.MM.YYYY'), --T_ENDDATE 
                                 pAmount                              --T_AMOUNT  
                               );
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          BEGIN
             RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Не найден лот с t_ID = '||pBuyID );
             RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20622,''); 
          END;
      END;
    END;


    --Вставляет или корректирует запись таблицы состояния
    --Не должна использоваться внутри триггеров по лотам.
    PROCEDURE CorrectTS (pType IN NUMBER, pBuyID IN NUMBER, pSaleID IN NUMBER, 
                        pDate IN DATE, pAmount IN NUMBER)
    IS
    BEGIN

      IF pAmount = 0 THEN
         RETURN;
      END IF;

      IF CorrectExistTS (pType, pBuyID, pSaleID, pDate, pAmount) = FALSE THEN
         CreateTS (pType, pBuyID, pSaleID, pDate, pAmount);
      END IF;

    END; --CorrectTS

    --Выполняет удаление записи таблицы состояния
    PROCEDURE EreaseTS (pID IN NUMBER, pBegDate IN DATE, pDate IN DATE)
    IS
      v_Amount NUMBER;
    BEGIN
      IF pBegDate > pDate THEN
         RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Некорректая дата начала записи в таблице состояний' );
         RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20623,''); 
      ELSIF pBegDate = pDate THEN
         DELETE 
           FROM DNPTXTS_DBT 
          WHERE t_ID = pID;
      ELSE
         UPDATE dnptxts_dbt
            SET t_EndDate = pDate
          WHERE t_ID = pID;
      END IF;

    END; --EreaseTS

    --Выполняет обработку таблицы состояния при создании/обновлении связи
    --Не должна использоваться внутри триггеров по лотам.
    PROCEDURE UpdateTSByLink (pLinkType IN NUMBER, pBuyID IN NUMBER,  pSaleID IN NUMBER, 
                             pSourceID IN NUMBER, pLinkDate IN DATE, pAmount IN NUMBER)
    IS
    BEGIN
      IF pLinkType = RSI_NPTXC.NPTXLNK_DELIVER THEN
         CorrectTS (RSI_NPTXC.NPTXTS_REST, pBuyID, 0, pLinkDate, -pAmount);
         UpdateTSBuyByLink (pLinkType, pBuyID, pSaleID, pLinkDate, TO_DATE('01.01.0001', 'DD.MM.YYYY'), pAmount);
      ELSIF pLinkType = RSI_NPTXC.NPTXLNK_REPO THEN
         CorrectTS (RSI_NPTXC.NPTXTS_REST, pBuyID, 0, pLinkDate, -pAmount);

         CorrectTS (RSI_NPTXC.NPTXTS_INVST, pBuyID, pSaleID, pLinkDate, pAmount);

      ELSIF pLinkType = RSI_NPTXC.NPTXLNK_OPPOS THEN
         CorrectTS (RSI_NPTXC.NPTXTS_REST, pBuyID, 0, pLinkDate, -pAmount);

         CorrectTS (RSI_NPTXC.NPTXTS_SHPOS, pBuyID, pSaleID, pLinkDate, pAmount);

      ELSIF pLinkType = RSI_NPTXC.NPTXLNK_SUBSTREPO THEN
         CorrectTS (RSI_NPTXC.NPTXTS_REST, pBuyID, 0, pLinkDate, -pAmount);

         CorrectTS (RSI_NPTXC.NPTXTS_INVST, pBuyID, pSaleID, pLinkDate, pAmount);

         CorrectTS (RSI_NPTXC.NPTXTS_REST, pSourceID, 0, pLinkDate, pAmount);

         CorrectTS (RSI_NPTXC.NPTXTS_INVST, pSourceID, pSaleID, pLinkDate, -pAmount);

      ELSIF pLinkType = RSI_NPTXC.NPTXLNK_CLPOS THEN
         CorrectTS (RSI_NPTXC.NPTXTS_REST, pBuyID, 0, pLinkDate, -pAmount);

         CorrectTS (RSI_NPTXC.NPTXTS_SHPOS, pSourceID, pSaleID, pLinkDate, -pAmount);

         CorrectTS (RSI_NPTXC.NPTXTS_REST, pSourceID, 0, pLinkDate, pAmount);

         UpdateTSBuyByLink (pLinkType, pBuyID, pSaleID, pLinkDate, TO_DATE('01.01.0001', 'DD.MM.YYYY'), pAmount);
      END IF;

    END;

    --Выполняет обработку таблицы состояния при выбытии 2 ч ОР
    --Выполняется в триггере лота.
    PROCEDURE UpdateTSByReverseRepo (pRepoID IN NUMBER, pDate IN DATE)
    IS
      CURSOR cT IS
                    SELECT *
                      FROM DNPTXTS_DBT
                     WHERE T_TYPE   = RSI_NPTXC.NPTXTS_REST
                       AND T_BUYID  = pRepoID
                       AND T_SALEID = 0
                       AND (T_ENDDATE = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR T_ENDDATE > pDate);

    BEGIN
      FOR T IN cT LOOP
         EreaseTS(T.T_ID, T.T_BEGDATE, pDate);
      END LOOP;

    END; --UpdateTSByReverseRepo

    --Выполняет обработку таблицы состояния при выбытии 2 ч ПР
    --Выполняется в триггере лота.
    PROCEDURE UpdateTSByDirectRepo (pRepoID IN NUMBER, pDate IN DATE)
    IS
      CURSOR cT IS
                   SELECT *
                     FROM DNPTXTS_DBT
                    WHERE T_TYPE   = RSI_NPTXC.NPTXTS_INVST
                      AND T_SALEID = pRepoID
                      AND (T_ENDDATE = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR T_ENDDATE > pDate);
    BEGIN
      FOR T IN cT LOOP
         EreaseTS(T.T_ID, T.T_BEGDATE, pDate);

         IF CorrectExistTS (RSI_NPTXC.NPTXTS_REST, T.t_BuyID, 0, pDate, T.t_Amount) = FALSE THEN
            INSERT INTO dnptxts_dbt( T_CLIENT   ,
                                     T_CONTRACT ,
                                     T_FIID     ,
                                     T_BUYID    ,
                                     T_SALEID   ,
                                     T_TYPE     ,
                                     T_BEGDATE  ,
                                     T_ENDDATE  ,
                                     T_AMOUNT  
                                   )
                             VALUES(
                                     T.t_Client,                          --T_CLIENT  
                                     T.t_Contract,                        --T_CONTRACT
                                     T.t_FIID,                            --T_FIID    
                                     T.t_BuyID,                           --T_BUYID   
                                     0,                                   --T_SALEID  
                                     RSI_NPTXC.NPTXTS_REST,               --T_TYPE    
                                     pDate,                               --T_BEGDATE 
                                     TO_DATE('01.01.0001', 'DD.MM.YYYY'), --T_ENDDATE 
                                     T.t_Amount                           --T_AMOUNT  
                                   );
         END IF;

      END LOOP;

    END; --UpdateTSByDirectRepo

    -- получить следующую ближайшую дату для связывания
    FUNCTION GetNextDate( v_Date_in IN DATE, pClient IN NUMBER , v_IIS IN NUMBER DEFAULT 0, pContract IN NUMBER DEFAULT 0 )
    RETURN DATE
    AS
      v_Date     DATE;
      v_BuyDate  DATE;
      v_SaleDate DATE;
      v_GOBuyDate DATE;
      v_GOSaleDate DATE;
    BEGIN
      v_Date := NULL;

      -- берем min из v_BuyDate, v_SaleDate
      SELECT MIN(Lot.t_BuyDate)
        INTO v_BuyDate
        FROM dnptxlot_dbt Lot
       WHERE Lot.t_BuyDate > v_Date_in
         AND Lot.t_Client = pClient
         AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = v_IIS
         AND ((pContract IS NULL) OR (pContract <= 0) OR (Lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)));

      SELECT MIN(Lot.t_SaleDate)
        INTO v_SaleDate
        FROM dnptxlot_dbt Lot
       WHERE Lot.t_SaleDate > v_Date_in
         AND Lot.t_Client = pClient
         AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = v_IIS
         AND ((pContract IS NULL) OR (pContract <= 0) OR (Lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)));

      SELECT MIN(txgo.t_BuyDate)
        INTO v_GOBuyDate
        FROM dnptxgo_dbt txgo
       WHERE txgo.t_BuyDate > v_Date_in;

      SELECT MIN(txgo.t_SaleDate)
        INTO v_GOSaleDate
        FROM dnptxgo_dbt txgo
       WHERE txgo.t_SaleDate > v_Date_in;


      IF v_BuyDate IS NOT NULL AND v_SaleDate IS NOT NULL THEN
        v_Date := iif(v_BuyDate < v_SaleDate, v_BuyDate, v_SaleDate);
      ELSIF v_BuyDate IS NULL AND v_SaleDate IS NOT NULL THEN
        v_Date := v_SaleDate;
      ELSIF v_SaleDate IS NULL AND v_BuyDate IS NOT NULL THEN
        v_Date := v_BuyDate;
      END IF;
      
      IF v_Date IS NULL THEN
        IF v_GOBuyDate IS NOT NULL AND v_GOSaleDate IS NOT NULL THEN
          v_Date := iif(v_GOBuyDate < v_GOSaleDate, v_GOBuyDate, v_GOSaleDate);
        ELSIF v_GOBuyDate IS NULL AND v_GOSaleDate IS NOT NULL THEN
          v_Date := v_GOSaleDate;
        ELSIF v_GOSaleDate IS NULL AND v_GOBuyDate IS NOT NULL THEN
          v_Date := v_GOBuyDate;
        END IF;
      ELSE
        IF v_GOBuyDate IS NOT NULL THEN
          v_Date := iif(v_GOBuyDate < v_Date, v_GOBuyDate, v_Date);
        END IF;
        IF v_GOSaleDate IS NOT NULL THEN
          v_Date := iif(v_GOSaleDate < v_Date, v_GOSaleDate, v_Date);
        END IF;  
      END IF;

      RETURN v_Date;
    END;

    -- Заполняет таблицы налогового учета
    -- Выполняется в транзакции шага. При возникновении ошибки транзакция не закрывается.
    PROCEDURE CreateLots( pOperDate IN DATE, 
                          pClient IN NUMBER, 
                          pIIS IN CHAR DEFAULT CHR(0), 
                          pFIID IN NUMBER DEFAULT -1, 
                          pBegDate IN DATE DEFAULT TO_DATE('01.01.0001','DD.MM.YYYY'), 
                          pRecalc IN CHAR DEFAULT CHR(0), 
                          pID_Operation IN NUMBER DEFAULT 0, 
                          pID_Step IN NUMBER DEFAULT 0, 
                          pContract  IN NUMBER DEFAULT 0)
    IS
       v_BegDate   DATE;
       v_BegDate0  DATE;
       v_BegDateGO DATE;
       v_EndDate   DATE;
       v_Date      DATE;
       v_FIID      NUMBER;
       v_SaleLots  dnptxlot_dbt%ROWTYPE;
       v_SaleLots2 dnptxlot_dbt%ROWTYPE;
       v_SaleLots3 dnptxlot_dbt%ROWTYPE;
       TYPE SaleLotsCurTyp IS REF CURSOR;
       c_SaleLots  SaleLotsCurTyp;
       c_SaleLots2 SaleLotsCurTyp;
       c_SaleLots3 SaleLotsCurTyp;
       v_IIS       NUMBER; 


       CURSOR c_CompRepo (v_Date IN DATE, v_FIID IN NUMBER) IS
                  SELECT *
                    FROM dnptxlot_dbt Lot
                   WHERE Lot.t_SaleDate = v_Date
                     AND Lot.t_Kind IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)
                     AND Lot.t_Client = pClient
                     AND Lot.t_FIID = v_FIID
                     AND Lot.t_Type = RSI_NPTXC.NPTXDEAL_COMP
                     AND Lot.t_RetFlag <> CHR(88);

       CURSOR c_CompBackRepo (v_Date IN DATE, v_FIID IN NUMBER) IS
                  SELECT *
                    FROM dnptxlot_dbt Lot
                   WHERE Lot.t_BuyDate = v_Date
                     AND Lot.t_Kind IN (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET)
                     AND Lot.t_Client = pClient
                     AND Lot.t_FIID = v_FIID
                     AND Lot.t_Type = RSI_NPTXC.NPTXDEAL_COMP
                     AND Lot.t_RetFlag <> CHR(88);

       CURSOR c_SaleRepoLots (v_Date IN DATE, v_FIID IN NUMBER) IS
                  SELECT lot.t_ID
                    FROM dnptxlot_dbt lot
                   WHERE lot.t_Kind IN ( RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT )
                     AND lot.t_BuyDate = v_Date
                     AND lot.t_BuyDate <> lot.t_SaleDate -- однодневки пропускаем
                     AND lot.t_Client = pClient
                     AND lot.t_FIID = v_FIID
                     AND lot.t_RetFlag <> CHR(88);

    BEGIN
      GetSettingsTax;

      v_IIS := iif( (pIIS = CHR(0) ), 0, 1);

      RSI_TRG_DNPTXLOT_DBT.v_ID_Operation := pID_Operation;
      RSI_TRG_DNPTXLOT_DBT.v_ID_Step      := pID_Step;
      RSI_TRG_DNPTXLOT_DBT.v_IsCreateLots := True;

      if( pBegDate != TO_DATE('01.01.0001','DD.MM.YYYY') )then
         v_BegDate0 := pBegDate;
      else
         v_BegDate0 := RSI_NPTO.GetCalcPeriodDate(RSI_NPTXC.NPTXCALC_CALCLINKS, pClient, pIIS, 0, pContract);
      end if;
      if v_BegDate0 = TO_DATE('01.01.0001','DD.MM.YYYY') then
         v_BegDate := TO_DATE('01.01.'||TO_NUMBER(TO_CHAR( pOperDate, 'YYYY')),'DD.MM.YYYY');
      else
         v_BegDate := v_BegDate0;
      end if;

      IF (v_BegDate > pOperDate) THEN
         RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Неверный диапазон дат' );
         RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20624,''); 
      END IF;

      v_EndDate := pOperDate;

      SELECT NVL(MIN(comm.t_CommDate), v_BegDate)
        INTO v_BegDateGO
        FROM DDL_COMM_DBT comm
       WHERE comm.T_DOCKIND IN (135, 138, 139) --(Глобальная операция с ц/б, Изменение номинала ц/б)
         AND comm.T_COMMDATE  <= v_BegDate
         AND comm.T_CommStatus = 2 --Закрыта
         AND NOT EXISTS (SELECT 1 FROM DNPTXGO_DBT G WHERE G.T_DOCKIND = comm.T_DOCKIND AND G.T_DOCUMENTID = comm.T_DOCUMENTID);

      InsertGO(v_BegDateGO, v_EndDate, pClient, v_IIS, pFIID, pContract);

      --В случае ошибки прекратить выполнение.
      IF ( pRecalc = CHR(0) ) THEN
         RSI_NPTO.SetCalcPeriodDate(RSI_NPTXC.NPTXCALC_CALCLINKS, pClient, v_EndDate, 1, pIIS, 0, pContract );
      END IF;

      InsertLots(v_BegDate, v_EndDate, pClient, v_IIS, pFIID, pContract);  

      -- Выполняем списание
      v_Date := v_BegDateGO;
      WHILE v_Date <= v_EndDate LOOP

         RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Выполняем списание v_Date = '||v_Date);

         FOR G IN (SELECT G.* FROM DNPTXGO_DBT G
                    WHERE G.T_SALEDATE = v_Date
                  ) LOOP

            ProcessGO(G, pClient, v_IIS, pFIID, pContract);  

         END LOOP;

         FOR G IN (SELECT G.* FROM DNPTXGO_DBT G
                    WHERE G.T_BUYDATE = v_Date
                      AND G.T_KIND = RSI_NPTXC.NPTXLOTORIGIN_GO --ГО
                  ) LOOP
            ProcessGOFI(G, pClient, v_IIS, pFIID, pContract);  

         END LOOP;


         -- Цикл по бумагам. Работаем в этот день только с теми, по которым в этот день было движение.
         /* -- Оставил для истории 
           FOR v_FI IN (SELECT av.*
                        FROM davoiriss_dbt av
                       WHERE av.t_FIID = case when pFIID != -1 then pFIID else av.t_FIID end
                         AND (EXISTS ( SELECT Lot.t_ID
                                         FROM dnptxlot_dbt Lot
                                        WHERE (Lot.t_BuyDate = v_Date OR Lot.t_SaleDate = v_Date)
                                          AND Lot.t_FIID = av.t_FIID
                                          AND Lot.t_Client = pClient
                                     ) OR
                              EXISTS ( SELECT txgo.t_ID
                                         FROM dnptxgo_dbt txgo
                                        WHERE (txgo.t_SaleDate = v_Date OR txgo.t_BuyDate = v_Date)
                                          AND (av.t_FIID = txgo.t_FIID OR av.t_FIID IN (select gofi.t_NewFIID 
                                                                                          from dnptxgofi_dbt gofi
                                                                                         where gofi.t_GOID = txgo.t_ID
                                                                                       )
                                              )
                                     )
                             )
                     ) LOOP */
         FOR v_FI IN (with txgo as
                       (select /*+ materialize */
                         txgo.t_ID, txgo.t_FIID
                          from dnptxgo_dbt txgo
                         where txgo.t_SaleDate = v_Date
                            or txgo.t_BuyDate = v_Date)
                      select av.*
                        from davoiriss_dbt av
                       where av.t_FIID = case when pFIID != -1 then pFIID else av.t_FIID end
                         and av.t_FIID in (select /*+ index(lot DNPTXLOT_DBT_USR1)*/
                                             Lot.t_FIID
                                              from dnptxlot_dbt Lot
                                             where Lot.t_BuyDate = v_Date
                                               and Lot.t_Client = pClient
                                            union
                                            select /*+ index(lot DNPTXLOT_DBT_USR2)*/
                                             Lot.t_FIID
                                              from dnptxlot_dbt Lot
                                             where Lot.t_SaleDate = v_Date
                                               and Lot.t_Client = pClient
                                             union 
                                             select t_FIID from txgo
                                             union  
                                             select gofi.t_NewFIID 
                                                  from dnptxgofi_dbt gofi 
                                                  where gofi.t_GOID in (select t_ID from txgo))
                     ) LOOP
            v_FIID := v_FI.t_FIID;

            if (v_Date > RSI_NPTXC.NPTX_ENDDATE2011) then
               UPDATE dnptxlot_dbt Lot
                  SET Lot.t_RetFlag = CHR(88)
                WHERE Lot.t_SaleDate = v_Date
                  AND Lot.t_Kind IN (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET)
                  AND Lot.t_Client = pClient
                  AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = v_IIS  
                  AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                  AND Lot.t_BuyDate <> lot.t_SaleDate
                  AND Lot.t_FIID = v_FIID
                  AND Lot.t_RetFlag <> CHR(88);
            end if;

            FOR Lots IN c_SaleRepoLots(v_Date, v_FIID) LOOP

               UPDATE dnptxlot_dbt lot
                  SET lot.t_RetFlag = CHR(88)
                WHERE lot.t_ID = Lots.t_ID;

               UPDATE dnptxlnk_dbt lnk
                  SET lnk.t_RetFlag = CHR(88)
                WHERE lnk.t_Type IN (RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO)
                  AND lnk.t_SaleID = Lots.t_ID;
            END LOOP;

            RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_DEBUG, 'Взведен t_RetFlag в связях');

            FOR CompRepo IN c_CompRepo(v_Date, v_FIID) LOOP
               ProcessCompPayOnDirectRepo(CompRepo);
            END LOOP;

            if (v_Date > RSI_NPTXC.NPTX_ENDDATE2011 and ReestrValue.W2 = RSI_NPTXC.NPTXREG_W2_YES) then
               Shuffling(v_Date, pClient, v_FIID, v_IIS, pContract);  
            end if;

            if (v_Date <= RSI_NPTXC.NPTX_ENDDATE2011) then
               OPEN c_SaleLots2 FOR SELECT /*+ INDEX( lot DNPTXLOT_DBT_IDXA)*/ *
                                      FROM dnptxlot_dbt lot
                                     WHERE lot.t_SaleDate = v_Date
                                       AND lot.t_Kind IN (RSI_NPTXC.NPTXLOTS_LOANGET, RSI_NPTXC.NPTXLOTS_BACKREPO)
                                       AND lot.t_Client   = pClient
                                       AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = v_IIS  
                                       AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                       AND lot.t_FIID     = v_FIID
                                       AND lot.t_RetFlag <> CHR(88)
                                  ORDER BY lot.t_BegSaleDate ASC,
                                           lot.t_DealDate ASC,
                                           lot.t_DealTime ASC,
                                           NLSSORT(lot.t_SortCode) ASC,
                                           lot.t_ID ASC;
               LOOP
                  FETCH c_SaleLots2 INTO v_SaleLots2;
                  EXIT WHEN c_SaleLots2%NOTFOUND OR
                            c_SaleLots2%NOTFOUND IS NULL;

                  -- Выполнить принудительное ЗКП
                  IF ((v_SaleLots2.t_BuyDate <> v_SaleLots2.t_SaleDate) and
                      ((v_SaleLots2.t_Blocked = CHR(0)) OR (ReestrValue.W1 = RSI_NPTXC.NPTXREG_W1_YES))
                     ) THEN
                    LinkPart2ToBuy(v_SaleLots2);
                  END IF;

                  UPDATE dnptxlot_dbt
                     SET t_RetFlag = CHR(88)
                   WHERE t_ID = v_SaleLots2.t_ID;

                  UpdateTSByReverseRepo (v_SaleLots2.t_ID, v_SaleLots2.t_SaleDate);
               END LOOP;

               CLOSE c_SaleLots2;
            end if;
     
            OPEN c_SaleLots FOR SELECT /*+ INDEX( lot DNPTXLOT_DBT_IDXA)*/ *
                                  FROM dnptxlot_dbt lot
                                 WHERE lot.t_SaleDate = v_Date
                                   AND lot.t_Kind = RSI_NPTXC.NPTXLOTS_SALE
                                   AND lot.t_Origin = RSI_NPTXC.NPTXLOTORIGIN_DEAL
                                   AND lot.t_Client = pClient
                                   AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = v_IIS  
                                   AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                   AND lot.t_FIID   = v_FIID
                                   AND (lot.t_Amount - lot.t_Sale) > 0
                              ORDER BY lot.t_BegSaleDate ASC,
                                       lot.t_DealDate ASC,
                                       lot.t_DealTime ASC,
                                       NLSSORT(lot.t_SortCode) ASC,
                                       lot.t_ID ASC;
            LOOP
               FETCH c_SaleLots INTO v_SaleLots;
               EXIT WHEN c_SaleLots%NOTFOUND OR
                         c_SaleLots%NOTFOUND IS NULL;

               LinkSale(v_SaleLots, v_IIS, pContract);  
            END LOOP;

            CLOSE c_SaleLots;


            if (v_Date <= RSI_NPTXC.NPTX_ENDDATE2011) then
               OPEN c_SaleLots3 FOR SELECT /*+ INDEX( lot DNPTXLOT_DBT_IDXA)*/ *
                                      FROM dnptxlot_dbt lot
                                     WHERE lot.t_SaleDate = v_Date
                                       AND lot.t_Kind IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)
                                       AND lot.t_Client = pClient
                                       AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = v_IIS  
                                       AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                       AND lot.t_FIID   = v_FIID
                                       AND lot.t_Type  <> RSI_NPTXC.NPTXDEAL_COMP
                                       AND lot.t_RetFlag <> CHR(88)
                                       AND (lot.t_Amount - lot.t_Sale) > 0
                                       AND lot.T_SALEDATE != lot.T_BUYDATE
                                  ORDER BY lot.t_BegSaleDate ASC,
                                           lot.t_DealDate ASC,
                                           lot.t_DealTime ASC,
                                           NLSSORT(lot.t_SortCode) ASC,
                                           lot.t_ID ASC;
               LOOP
                  FETCH c_SaleLots3 INTO v_SaleLots3;
                  EXIT WHEN c_SaleLots3%NOTFOUND OR
                            c_SaleLots3%NOTFOUND IS NULL;

                  LinkDirectRepo(v_SaleLots3, v_IIS, pContract);  
               END LOOP;

               CLOSE c_SaleLots3;
            end if;

            CloseShortPos(v_Date, pClient, v_FIID, v_IIS, pContract);  

            FOR CompBackRepo IN c_CompBackRepo(v_Date, v_FIID) LOOP
               ProcessCompPayOnReverseRepo(CompBackRepo, v_IIS);   
            END LOOP;

            if (v_Date > RSI_NPTXC.NPTX_ENDDATE2011) then
               OPEN c_SaleLots2 FOR SELECT /*+ INDEX( lot DNPTXLOT_DBT_IDXA)*/ *
                                      FROM dnptxlot_dbt lot
                                     WHERE lot.t_SaleDate = v_Date
                                       AND lot.t_Kind IN (RSI_NPTXC.NPTXLOTS_LOANGET, RSI_NPTXC.NPTXLOTS_BACKREPO)
                                       AND lot.t_Client   = pClient
                                       AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = v_IIS 
                                       AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                       AND lot.t_FIID   = v_FIID
                                  ORDER BY lot.t_BegSaleDate ASC,
                                           lot.t_DealDate ASC,
                                           lot.t_DealTime ASC,
                                           NLSSORT(lot.t_SortCode) ASC,
                                           lot.t_ID ASC;
               LOOP
                  FETCH c_SaleLots2 INTO v_SaleLots2;
                  EXIT WHEN c_SaleLots2%NOTFOUND OR
                            c_SaleLots2%NOTFOUND IS NULL;

                  -- Выполнить принудительное ЗКП
                  IF ((v_SaleLots2.t_BuyDate <> v_SaleLots2.t_SaleDate) and
                      ((v_SaleLots2.t_Blocked = CHR(0)) OR (ReestrValue.W1 = RSI_NPTXC.NPTXREG_W1_YES))
                     ) THEN
                    LinkPart2ToBuy(v_SaleLots2, v_IIS, pContract);  
                  END IF;

                  if v_SaleLots2.t_RetFlag <> CHR(88) then
                     UPDATE dnptxlot_dbt
                        SET t_RetFlag = CHR(88)
                      WHERE t_ID = v_SaleLots2.t_ID;
                  end if;

                  UpdateTSByReverseRepo (v_SaleLots2.t_ID, v_SaleLots2.t_SaleDate);
               END LOOP;

               CLOSE c_SaleLots2;
            end if;
     
            if (v_Date > RSI_NPTXC.NPTX_ENDDATE2011) then
               OPEN c_SaleLots3 FOR SELECT /*+ INDEX( lot DNPTXLOT_DBT_IDXA)*/ *
                                      FROM dnptxlot_dbt lot
                                     WHERE lot.t_SaleDate = v_Date
                                       AND lot.t_Kind IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)
                                       AND lot.t_Client = pClient
                                       AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = v_IIS 
                                       AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                       AND lot.t_FIID   = v_FIID
                                       AND lot.t_Type  <> RSI_NPTXC.NPTXDEAL_COMP
                                       AND lot.t_RetFlag <> CHR(88)
                                       AND (lot.t_Amount - lot.t_Sale) > 0
                                       AND lot.T_SALEDATE != lot.T_BUYDATE
                                  ORDER BY lot.t_BegSaleDate ASC,
                                           lot.t_DealDate ASC,
                                           lot.t_DealTime ASC,
                                           NLSSORT(lot.t_SortCode) ASC,
                                           lot.t_ID ASC;
               LOOP
                  FETCH c_SaleLots3 INTO v_SaleLots3;
                  EXIT WHEN c_SaleLots3%NOTFOUND OR
                            c_SaleLots3%NOTFOUND IS NULL;

                  LinkDirectRepo(v_SaleLots3, v_IIS, pContract);  
               END LOOP;

               CLOSE c_SaleLots3;
            end if;
         END LOOP;

         v_Date := GetNextDate( v_Date, pClient, v_IIS, pContract );  

         EXIT WHEN v_Date IS NULL;

      END LOOP;

      RSI_TRG_DNPTXLOT_DBT.v_ID_Operation := 0;
      RSI_TRG_DNPTXLOT_DBT.v_ID_Step      := 0;
      RSI_TRG_DNPTXLOT_DBT.v_IsCreateLots := False;
    END;

------------------------------------------------------------------------------------------------
    -- Процедуры отката

    -- Процедура отката вставки ГО  
    PROCEDURE RecoilInsertGO( pEndDate IN DATE, pBegDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0  )
    IS
       v_PrevDate   DATE;
    BEGIN
       v_PrevDate := RSI_NPTO.GetMaxCalcPeriodDate(RSI_NPTXC.NPTXCALC_CALCLINKS);

       IF pEndDate > v_PrevDate THEN 

         IF v_PrevDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
            DELETE FROM DNPTXGO_DBT WHERE T_DOCKIND in(135,139);--Глобальная операция, Изменение номинала
         ELSE
            DELETE FROM DNPTXGO_DBT
             WHERE T_SALEDATE > v_PrevDate 
               AND T_DOCKIND in(135,139);--Глобальная операция, Изменение номинала

            DELETE FROM DNPTXGOFI_DBT
             WHERE T_GOID IN (SELECT T_ID
                                FROM DNPTXGO_DBT
                               WHERE T_BUYDATE > v_PrevDate
                                 AND T_DOCKIND = 135--Глобальная операция
                             );
         END IF;
       END IF;

       DELETE FROM DNPTXGO_DBT G
        WHERE G.T_SALEDATE >= pBegDate
          AND G.T_DOCKIND = RSI_NPTXC.DL_CONVAVR
          AND G.t_FIID = case when pFIID != -1 then pFIID else G.t_FIID end
          AND EXISTS (SELECT 1 FROM DDL_TICK_DBT TK 
                       WHERE TK.T_DEALID = G.T_DOCUMENTID 
                         AND TK.T_CLIENTID = pClient
                         AND RSI_NPTO.CheckContrIIS(TK.T_CLIENTCONTRID) = pIIS
                         AND ((pContract IS NULL) OR (pContract <= 0) OR (TK.T_CLIENTCONTRID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract))));

       DELETE FROM DNPTXGOFI_DBT GF
        WHERE EXISTS (SELECT 1 FROM DNPTXGO_DBT G, DDL_TICK_DBT TK
                       WHERE G.T_ID = GF.T_GOID
                         AND G.T_BUYDATE >= pBegDate
                         AND G.T_DOCKIND = RSI_NPTXC.DL_CONVAVR
                         AND TK.T_DEALID = G.T_DOCUMENTID
                         AND TK.T_CLIENTID = pClient
                         AND RSI_NPTO.CheckContrIIS(TK.T_CLIENTCONTRID) = pIIS
                         AND ((pContract IS NULL) OR (pContract <= 0) OR (TK.T_CLIENTCONTRID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                         AND TK.t_PFI = case when pFIID != -1 then pFIID else TK.t_PFI end
                     );
          

       UPDATE DNPTXGO_DBT G
             SET G.T_BUYDATE = TO_DATE('01.01.0001','DD.MM.YYYY')
           WHERE G.T_DOCKIND = RSI_NPTXC.DL_CONVAVR
             AND G.T_SALEDATE < pBegDate
             AND G.T_BUYDATE >= pBegDate
             AND G.t_FIID = case when pFIID != -1 then pFIID else G.t_FIID end
             AND EXISTS(SELECT 1 FROM DDL_TICK_DBT TK
                         WHERE TK.T_DEALID = G.T_DOCUMENTID
                           AND TK.T_CLIENTID = pClient
                           AND RSI_NPTO.CheckContrIIS(TK.T_CLIENTCONTRID) = pIIS
                           AND ((pContract IS NULL) OR (pContract <= 0) OR (TK.T_CLIENTCONTRID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                       );


    END;

    -- Процедура отката вставки лотов
    PROCEDURE RecoilInsertLots( pBegDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0 )  
    IS
      CURSOR CompLots IS
                         SELECT lot.t_ID
                           FROM dnptxlot_dbt lot
                          WHERE lot.t_Type = RSI_NPTXC.NPTXDEAL_COMP
                            AND lot.t_InAcc  = CHR(88)
                            AND lot.t_Client = pClient
                            AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS
                            AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                            AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end
                            AND ( ( lot.T_KIND IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT) AND
                                    lot.T_SALEDATE >= pBegDate
                                  ) OR
                                  ( lot.T_KIND IN (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET) AND
                                    lot.T_BUYDATE >= pBegDate
                                  )
                                );
      CURSOR LotsSalePart2 IS
             SELECT Lot.t_ID, Lot.t_SaleDate
               FROM dnptxlot_dbt Lot
              WHERE (Lot.t_Kind IN ( RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET ))
                AND (Lot.t_SaleDate >= pBegDate OR Lot.t_BegSaleDate >= pBegDate)
                AND lot.t_Client = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end;

      CURSOR LotsBuyPart2 IS
             SELECT Lot.t_ID, Lot.t_BuyDate
               FROM dnptxlot_dbt Lot
              WHERE (Lot.t_Kind IN ( RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT ))
                AND (Lot.t_BuyDate >= pBegDate OR Lot.t_BegBuyDate >= pBegDate)
                AND lot.t_Client = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end;

      CURSOR LotsWrtIn IS
             SELECT RQ.T_ID, RQ.T_DOCID, RQ.T_DOCKIND, DLSUM.T_DATE
               FROM DDLRQ_DBT RQ, DOPRKOPER_DBT KOPER, DDL_TICK_DBT TICK, DDL_LEG_DBT LEG, DDLSUM_DBT DLSUM
              WHERE RQ.T_FACTDATE  >= pBegDate                    
                AND RQ.T_STATE      = RSI_DLRQ.DLRQ_STATE_EXEC                    
                AND RQ.T_SUBKIND    = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS                          
                AND RQ.T_TYPE       = RSI_DLRQ.DLRQ_TYPE_DELIVERY                        
                AND RQ.t_FIID       = case when pFIID != -1 then pFIID else RQ.t_FIID end
                AND TICK.T_CLIENTID = pClient                  
                AND RSI_NPTO.CheckContrIIS(TICK.T_CLIENTCONTRID) = pIIS 
                AND ((pContract IS NULL) OR (pContract <= 0) OR (TICK.T_CLIENTCONTRID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND TICK.T_DEALID   = RQ.T_DOCID             
                AND TICK.T_BOFFICEKIND = RQ.T_DOCKIND      
                AND KOPER.T_KIND_OPERATION = TICK.T_DEALTYPE
                AND LEG.T_DEALID    = TICK.T_DEALID           
                AND LEG.T_LEGID     = 0                            
                AND LEG.T_LEGKIND   = 0                         
                AND RQ.T_DOCKIND    = RSI_NPTXC.DL_AVRWRT   
                AND DLSUM.T_DOCKIND = TICK.T_BOFFICEKIND      
                AND DLSUM.T_DOCID   = TICK.T_DEALID             
                AND DLSUM.T_KIND    = RSI_NPTXC.DLSUM_KIND_COSTWRTTAX       
                AND DLSUM.T_DATE    < pBegDate;                        

    BEGIN
       IF pBegDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
          DELETE FROM DNPTXLOT_DBT WHERE T_CLIENT = pClient 
                                     AND RSI_NPTO.CheckContrIIS(t_Contract)  = pIIS
                                     AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                     AND t_FIID = case when pFIID != -1 then pFIID else t_FIID end;
       ELSE
          FOR LC IN CompLots LOOP
            UPDATE dnptxlot_dbt
               SET t_BuyDate = t_BegBuyDate,
                   t_ChildID = 0
             WHERE t_ChildID = LC.t_ID
               AND t_Kind IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT)
               AND t_SaleDate < pBegDate;

            UPDATE dnptxlot_dbt
               SET t_SaleDate = t_BegSaleDate,
                   t_ChildID = 0
             WHERE t_ChildID = LC.t_ID
               AND t_Kind IN (RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET)
               AND t_BuyDate < pBegDate;
          END LOOP;

          DELETE
            FROM dnptxlot_dbt lot
           WHERE ( ( (lot.t_Kind IN ( RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET )) AND
                     (lot.t_BuyDate >= pBegDate or lot.t_BegBuyDate >= pBegDate) ) OR
                   ( (lot.t_Kind IN ( RSI_NPTXC.NPTXLOTS_SALE, RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT )) AND
                     (lot.t_SaleDate >= pBegDate)
                   )
                 )
             AND lot.t_Client = pClient
             AND RSI_NPTO.CheckContrIIS(lot.t_Contract)  = pIIS   
             AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
             AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end;

          DELETE
            FROM dnptxlot_dbt lot
           WHERE lot.t_Kind IN ( RSI_NPTXC.NPTXLOTS_BUY, RSI_NPTXC.NPTXLOTS_BACKREPO, RSI_NPTXC.NPTXLOTS_LOANGET ) 
             AND lot.t_GOID > 0
             AND EXISTS (SELECT 1
                           FROM DNPTXGO_DBT GL
                          WHERE GL.T_ID = lot.T_GOID
                            AND GL.T_BUYDATE >= pBegDate
                            AND EXTRACT(YEAR FROM lot.t_BegBuyDate) < EXTRACT(YEAR FROM GL.T_BUYDATE) 
                        )
             AND lot.t_Client = pClient
             AND RSI_NPTO.CheckContrIIS(lot.t_Contract) = pIIS  
             AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
             AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end;

          FOR WrtIn IN LotsWrtIn LOOP
             DELETE
               FROM dnptxlot_dbt lot
              WHERE lot.t_DOCKIND = WrtIn.t_DOCKIND
                AND lot.t_DOCID   = WrtIn.t_DOCID
                AND lot.t_RQID    = WrtIn.t_ID
                AND lot.t_Kind    = RSI_NPTXC.NPTXLOTS_BUY
                AND lot.t_BUYDATE = WrtIn.t_Date
                AND lot.t_Client  = pClient
                AND RSI_NPTO.CheckContrIIS(lot.t_Contract) = pIIS
                AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end;
          END LOOP;

          --Удалить лоты зачислений, по которым не существует уже самих зачислений и лоты не участвуют в связях
          DELETE FROM dnptxlot_dbt lot 
           WHERE lot.t_DocKind = RSI_NPTXC.DL_AVRWRT 
             AND lot.t_Kind    = RSI_NPTXC.NPTXLOTS_BUY
             AND lot.t_Client  = pClient
             AND RSI_NPTO.CheckContrIIS(lot.t_Contract) = pIIS
             AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
             AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end 
             AND NOT EXISTS(SELECT 1 FROM ddl_tick_dbt tk WHERE tk.t_DealID = lot.t_DocID) 
             AND NOT EXISTS(SELECT 1 FROM dnptxlnk_dbt lnk WHERE lnk.t_BuyID = lot.t_ID);

          --Удалить лоты списаний, по которым не существует уже самих списаний и лоты не участвуют в связях
          DELETE FROM dnptxlot_dbt lot 
           WHERE lot.t_DocKind = RSI_NPTXC.DL_AVRWRT 
             AND lot.t_Kind    = RSI_NPTXC.NPTXLOTS_SALE
             AND lot.t_Client  = pClient
             AND RSI_NPTO.CheckContrIIS(lot.t_Contract) = pIIS
             AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
             AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end 
             AND NOT EXISTS(SELECT 1 FROM ddl_tick_dbt tk WHERE tk.t_DealID = lot.t_DocID) 
             AND NOT EXISTS(SELECT 1 FROM dnptxlnk_dbt lnk WHERE lnk.t_SaleID = lot.t_ID);


          FOR SalePart2 IN LotsSalePart2 LOOP
            IF SalePart2.t_SaleDate >= pBegDate THEN
              UPDATE dnptxlot_dbt
                 SET t_BegSaleDate = TO_DATE('01.01.0001','DD.MM.YYYY'),
                     t_SaleDate = TO_DATE('01.01.0001','DD.MM.YYYY'),
                     t_RetFlag = CHR(0)
               WHERE t_ID = SalePart2.t_ID;
            ELSE
              UPDATE dnptxlot_dbt
                 SET t_BegSaleDate = TO_DATE('01.01.0001','DD.MM.YYYY')
               WHERE t_ID = SalePart2.t_ID;
            END IF;
          END LOOP;

          FOR BuyPart2 IN LotsBuyPart2 LOOP
            IF BuyPart2.t_BuyDate >= pBegDate THEN
              UPDATE dnptxlot_dbt
                 SET t_BegBuyDate = TO_DATE('01.01.0001','DD.MM.YYYY'),
                     t_BuyDate = TO_DATE('01.01.0001','DD.MM.YYYY'),
                     t_RetFlag = CHR(0)
               WHERE t_ID = BuyPart2.t_ID;
            ELSE
              UPDATE dnptxlot_dbt
                 SET t_BegBuyDate = TO_DATE('01.01.0001','DD.MM.YYYY')
               WHERE t_ID = BuyPart2.t_ID;
            END IF;
          END LOOP;

         UPDATE dnptxlot_dbt lot
            SET lot.t_childid = 0
          WHERE lot.t_childID not in (select t_ID from dnptxlot_dbt)
            AND lot.t_childID <> 0
            AND lot.t_Client = pClient
            AND RSI_NPTO.CheckContrIIS(lot.t_Contract) = pIIS
            AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
            AND lot.t_FIID = case when pFIID != -1 then pFIID else lot.t_FIID end;
       END IF;
    END;

    -- Процедура отката вставки связей
    PROCEDURE RecoilInsertLinks( pBegDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0 )    
    IS
    BEGIN
       IF pBegDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
          DELETE FROM DNPTXLNK_DBT WHERE T_CLIENT = pClient
                                     AND RSI_NPTO.CheckContrIIS(t_Contract) = pIIS
                                     AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                     AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end;
       ELSE

          -- сначала сбрасываем признак возврата в связях
          UPDATE dnptxlnk_dbt lnk
             SET lnk.t_RetFlag = CHR(0)
           WHERE lnk.t_RetFlag = CHR(88)
             AND lnk.t_Type IN (RSI_NPTXC.NPTXLNK_REPO, RSI_NPTXC.NPTXLNK_SUBSTREPO)
             AND lnk.t_Client = pClient
             AND RSI_NPTO.CheckContrIIS(lnk.t_Contract)  = pIIS
             AND ((pContract IS NULL) OR (pContract <= 0) OR (lnk.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
             AND lnk.T_FIID = case when pFIID != -1 then pFIID else lnk.T_FIID end
             AND (SELECT lot.t_Kind
                      FROM dnptxlot_dbt lot
                     WHERE lot.t_ID = lnk.t_SaleID
                       AND lot.t_BuyDate >= pBegDate
                 ) IN (RSI_NPTXC.NPTXLOTS_REPO, RSI_NPTXC.NPTXLOTS_LOANPUT);
          DELETE
            FROM dnptxlnk_dbt
           WHERE t_Date  >= pBegDate
             AND t_Client = pClient
             AND RSI_NPTO.CheckContrIIS(t_Contract)  = pIIS
             AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
             AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end;

       END IF;
    END;

    PROCEDURE RecoilInsertTSBuy( pBegDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0 )
    IS
      v_ID NUMBER := 0;
      v_Amount NUMBER := 0;
    BEGIN

       FOR TS IN (SELECT *
                    FROM dnptxts_dbt
                   WHERE t_EndDate >= pBegDate
                     AND t_SaleID = 0
                     AND t_Client = pClient
                     AND t_Type   = RSI_NPTXC.NPTXTS_BUY           
                     AND RSI_NPTO.CheckContrIIS(t_Contract) = pIIS
                     AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                     AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end
                    order by t_EndDate
                 ) LOOP

          BEGIN
           SELECT t_ID into v_ID
             FROM dnptxts_dbt
            WHERE t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
              AND t_BegDate = TS.t_EndDate
              AND t_BuyID   = TS.t_BuyID
              AND t_Client  = TS.t_Client
              AND t_Type    = RSI_NPTXC.NPTXTS_BUY;
          EXCEPTION
           WHEN NO_DATA_FOUND THEN v_ID := 0;
          END;

          if( v_ID > 0 )then
             UPDATE dnptxts_dbt
                SET t_Amount = t_Amount - TS.t_Amount 
              WHERE t_ID = v_ID;
          end if;

          BEGIN
           SELECT t_ID, t_Amount into v_ID, v_Amount
              FROM dnptxts_dbt
             WHERE t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
               AND t_BegDate = TS.t_BegDate
               AND t_BuyID   = TS.t_BuyID
               AND t_Client  = TS.t_Client
               AND t_Type    = RSI_NPTXC.NPTXTS_BUY;
          EXCEPTION
           WHEN NO_DATA_FOUND THEN v_ID := 0;
                               v_Amount := 0;
          END;

          if( v_ID > 0 )then
             DELETE
               FROM dnptxts_dbt
              WHERE t_ID = v_ID;

             UPDATE dnptxts_dbt
                SET t_Amount = t_Amount + v_Amount 
              WHERE t_ID = TS.T_ID;
          end if;

          FOR S IN (SELECT *
                     FROM dnptxts_dbt
                    WHERE t_EndDate >= pBegDate
                      AND t_BegDate = TS.t_BegDate
                      AND t_SaleID  > 0
                      AND t_BuyID   = TS.t_BuyID
                      AND t_Client  = TS.t_Client
                      AND t_Type    = RSI_NPTXC.NPTXTS_BUY
                   ) LOOP

             DELETE
               FROM dnptxts_dbt
              WHERE t_ID = S.T_ID;

             UPDATE dnptxts_dbt
                SET t_Amount = t_Amount + S.t_Amount 
              WHERE t_ID = TS.T_ID;

          END LOOP; 

       END LOOP; 

       FOR TS IN (SELECT *
                    FROM dnptxts_dbt
                   WHERE t_EndDate >= pBegDate
                     AND t_SaleID > 0
                     AND t_Client = pClient
                     AND t_Type   = RSI_NPTXC.NPTXTS_BUY           
                     AND RSI_NPTO.CheckContrIIS(t_Contract) = pIIS
                     AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                     AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end
                    order by t_EndDate
                 ) LOOP

          BEGIN
           SELECT t_ID into v_ID
             FROM dnptxts_dbt
            WHERE t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
              AND t_BegDate = TS.t_BegDate
              AND t_BuyID   = TS.t_BuyID
              AND t_Client  = TS.t_Client
              AND t_Type    = RSI_NPTXC.NPTXTS_BUY;
          EXCEPTION
           WHEN NO_DATA_FOUND THEN v_ID := 0;
          END;

          if( v_ID > 0 )then
             UPDATE dnptxts_dbt
                SET t_Amount = t_Amount + TS.t_Amount 
              WHERE t_ID = v_ID;

             DELETE
               FROM dnptxts_dbt
              WHERE t_ID = TS.T_ID;
          else
             UPDATE dnptxts_dbt
                SET t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY'),
                    t_SaleID = 0
              WHERE t_ID = TS.T_ID;
          end if;

       END LOOP;

       FOR TS IN ( SELECT *
                     FROM dnptxts_dbt ts
                    WHERE ts.t_Client  = pClient
                      AND ts.t_Type    = RSI_NPTXC.NPTXTS_BUY
                      AND RSI_NPTO.CheckContrIIS(ts.t_Contract) = pIIS
                      AND ((pContract IS NULL) OR (pContract <= 0) OR (ts.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                      AND ts.T_FIID = case when pFIID != -1 then pFIID else ts.T_FIID end
                      AND ts.t_BegDate = (select min(t.t_BegDate)
                                            from dnptxts_dbt t
                                           where t.t_BuyID = ts.t_BuyID
                                             and t.t_Client = ts.t_Client
                                             and t.t_Type   = RSI_NPTXC.NPTXTS_BUY
                                             and t.t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
                                             and t.t_BegDate < pBegDate
                                         )
                      AND ts.t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
                 ) LOOP

          BEGIN
           SELECT nvl(sum(t_Amount),0) into v_Amount
             FROM dnptxts_dbt
            WHERE t_BegDate >= pBegDate
              AND t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
              AND t_BuyID   = TS.t_BuyID
              AND t_Client  = TS.T_Client
              AND t_Type    = RSI_NPTXC.NPTXTS_BUY;
          EXCEPTION
           WHEN NO_DATA_FOUND THEN v_Amount := 0;
          END;
         
          UPDATE dnptxts_dbt
             SET t_Amount = t_Amount + v_Amount 
           WHERE t_ID   = TS.t_ID;

           DELETE 
             FROM dnptxts_dbt
            WHERE t_BegDate >= pBegDate
              AND t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
              AND t_BuyID   = TS.t_BuyID
              AND t_Client  = TS.T_Client
              AND t_Type    = RSI_NPTXC.NPTXTS_BUY;

       END LOOP; 

       FOR TS IN (SELECT *
                    FROM dnptxts_dbt
                   WHERE t_EndDate >= pBegDate
                     AND t_SaleID = 0
                     AND t_Client = pClient
                     AND t_Type   = RSI_NPTXC.NPTXTS_BUY           
                     AND RSI_NPTO.CheckContrIIS(t_Contract) = pIIS
                     AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                     AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end
                    order by t_EndDate
                 ) LOOP

          UPDATE dnptxts_dbt
             SET t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
           WHERE t_ID = TS.T_ID;

       END LOOP; 

       DELETE
         FROM dnptxts_dbt
        WHERE t_BegDate >= pBegDate
          AND t_Client  = pClient
          AND t_Type    = RSI_NPTXC.NPTXTS_BUY
          AND RSI_NPTO.CheckContrIIS(t_Contract) = pIIS
          AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
          AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end;

    END;

    -- Процедура отката вставки таблицы состояний  
    PROCEDURE RecoilInsertTS( pBegDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1, pContract IN NUMBER DEFAULT 0 )
    IS
      CURSOR LotsWrtIn IS
             SELECT RQ.T_ID, RQ.T_DOCID, RQ.T_DOCKIND, DLSUM.T_DATE
               FROM DDLRQ_DBT RQ, DOPRKOPER_DBT KOPER, DDL_TICK_DBT TICK, DDL_LEG_DBT LEG, DDLSUM_DBT DLSUM
              WHERE RQ.T_FACTDATE  >= pBegDate                    
                AND RQ.T_STATE      = RSI_DLRQ.DLRQ_STATE_EXEC                    
                AND RQ.T_SUBKIND    = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS                          
                AND RQ.T_TYPE       = RSI_DLRQ.DLRQ_TYPE_DELIVERY
                AND RQ.T_FIID       = case when pFIID != -1 then pFIID else RQ.T_FIID end         
                AND TICK.T_CLIENTID = pClient                  
                AND RSI_NPTO.CheckContrIIS(TICK.T_CLIENTCONTRID) = pIIS 
                AND ((pContract IS NULL) OR (pContract <= 0) OR (TICK.T_CLIENTCONTRID in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                AND TICK.T_DEALID   = RQ.T_DOCID             
                AND TICK.T_BOFFICEKIND = RQ.T_DOCKIND      
                AND KOPER.T_KIND_OPERATION = TICK.T_DEALTYPE
                AND LEG.T_DEALID    = TICK.T_DEALID           
                AND LEG.T_LEGID     = 0                            
                AND LEG.T_LEGKIND   = 0                         
                AND RQ.T_DOCKIND    = RSI_NPTXC.DL_AVRWRT   
                AND DLSUM.T_DOCKIND = TICK.T_BOFFICEKIND      
                AND DLSUM.T_DOCID   = TICK.T_DEALID             
                AND DLSUM.T_KIND    = RSI_NPTXC.DLSUM_KIND_COSTWRTTAX       
                AND DLSUM.T_DATE    < pBegDate;

      TYPE WrtInLots IS REF CURSOR;
      c_WrtInLots WrtInLots;
      v_WrtInLots dnptxlot_dbt%ROWTYPE;
                        
    BEGIN
       IF pBegDate = TO_DATE('01.01.0001','DD.MM.YYYY') THEN
          DELETE FROM DNPTXTS_DBT WHERE T_CLIENT = pClient 
                                    AND RSI_NPTO.CheckContrIIS(t_Contract)  = pIIS
                                    AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
                                    AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end;
       ELSE
          RecoilInsertTSBuy(pBegDate,pClient,pIIS,pFIID,pContract);

          DELETE
            FROM dnptxts_dbt
           WHERE t_BegDate >= pBegDate
             AND t_Client   = pClient
             AND t_Type != RSI_NPTXC.NPTXTS_BUY
             AND RSI_NPTO.CheckContrIIS(t_Contract)  = pIIS
             AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
             AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end;

          FOR WrtIn IN LotsWrtIn LOOP

             OPEN c_WrtInLots FOR SELECT lot.*
                                    FROM dnptxlot_dbt lot
                                   WHERE lot.t_DOCKIND = WrtIn.t_DOCKIND
                                     AND lot.t_DOCID   = WrtIn.t_DOCID
                                     AND lot.t_RQID    = WrtIn.t_ID
                                     AND lot.t_Kind    = RSI_NPTXC.NPTXLOTS_BUY
                                     AND lot.t_BUYDATE = WrtIn.t_Date
                                     AND lot.t_Client  = pClient
                                     AND RSI_NPTO.CheckContrIIS(lot.t_Contract) = pIIS
                                     AND ((pContract IS NULL) OR (pContract <= 0) OR (lot.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)));
             LOOP
                FETCH c_WrtInLots INTO v_WrtInLots;
                EXIT WHEN c_WrtInLots%NOTFOUND OR
                          c_WrtInLots%NOTFOUND IS NULL;

                DELETE
                  FROM DNPTXTS_DBT TS
                 WHERE TS.t_BUYID   = v_WrtInLots.t_ID
                   AND TS.t_BEGDATE = v_WrtInLots.t_BUYDATE
                   AND TS.t_Client  = pClient
                   AND RSI_NPTO.CheckContrIIS(TS.t_Contract) = pIIS
                   AND ((pContract IS NULL) OR (pContract <= 0) OR (TS.t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)));

             END LOOP;

             CLOSE c_WrtInLots;

          END LOOP;

          UPDATE dnptxts_dbt
             SET t_EndDate = TO_DATE('01.01.0001','DD.MM.YYYY')
           WHERE t_Client   = pClient
             AND RSI_NPTO.CheckContrIIS(t_Contract)  = pIIS
             AND ((pContract IS NULL) OR (pContract <= 0) OR (t_Contract in (SELECT MP.T_SFCONTRID FROM DDLCONTRMP_DBT MP WHERE MP.T_DLCONTRID = pContract)))
             AND t_Type != RSI_NPTXC.NPTXTS_BUY
             AND T_FIID = case when pFIID != -1 then pFIID else T_FIID end
             AND t_EndDate >= pBegDate;
       END IF;
    END;

    --Процедура восстановления данных НДФЛ из истории
    PROCEDURE RestoreNPTX(pID_Operation  IN NUMBER,
                          pID_Step       IN NUMBER)
    IS
      v_nptxlotbc  dnptxlotbc_dbt%rowtype;
      v_nptxlnkbc  dnptxlnkbc_dbt%rowtype;
      v_nptxlsbc   dnptxlsbc_dbt%rowtype;
      v_nptxtsbc   dnptxtsbc_dbt%rowtype;
      v_nptxgobc   dnptxgobc_dbt%rowtype;
      v_nptxgofibc dnptxgofibc_dbt%rowtype;
    BEGIN
      RSI_TRG_DNPTXLOT_DBT.v_ID_Operation := 0;
      RSI_TRG_DNPTXLOT_DBT.v_ID_Step      := 0;
      RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC  := TRUE;

      FOR one_nptxbc IN (SELECT t_BCID, t_Action, t_BackObjID, t_ObjKind, t_ObjID
                           FROM dnptxbc_dbt
                          WHERE t_ID_Operation = pID_Operation
                            AND t_ID_Step      = pID_Step
                          ORDER BY t_BCID DESC
                        )
      LOOP

        IF one_nptxbc.t_Action = RSI_NPTXC.NPTXBC_ACTION_CREATE THEN

          IF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LOT THEN

            DELETE FROM DNPTXLOT_DBT WHERE T_ID = one_nptxbc.t_ObjID;

          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LNK THEN 

            DELETE FROM DNPTXLNK_DBT WHERE T_ID = one_nptxbc.t_ObjID;

          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LS THEN

            DELETE FROM DNPTXLS_DBT WHERE T_ID = one_nptxbc.t_ObjID;
           
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_TS THEN

            DELETE FROM DNPTXTS_DBT WHERE T_ID = one_nptxbc.t_ObjID;
           
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_GO THEN

            DELETE FROM DNPTXGO_DBT WHERE T_ID = one_nptxbc.t_ObjID;
           
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_GOFI THEN

            DELETE FROM DNPTXGOFI_DBT WHERE T_ID = one_nptxbc.t_ObjID;

          END IF;

        ELSIF one_nptxbc.t_Action = RSI_NPTXC.NPTXBC_ACTION_UPDATE THEN
          
          IF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LOT THEN
            BEGIN
              SELECT * INTO v_nptxlotbc
                FROM DNPTXLOTBC_DBT
               WHERE t_LotBCID = one_nptxbc.t_BackObjID;
            
              UPDATE DNPTXLOT_DBT
                 SET T_DOCKIND         = v_nptxlotbc.T_DOCKIND        ,
                     T_DOCID           = v_nptxlotbc.T_DOCID          ,
                     T_DEALDATE        = v_nptxlotbc.T_DEALDATE       ,
                     T_DEALTIME        = v_nptxlotbc.T_DEALTIME       ,
                     T_DEALCODE        = v_nptxlotbc.T_DEALCODE       ,
                     T_DEALCODETS      = v_nptxlotbc.T_DEALCODETS     ,
                     T_CLIENT          = v_nptxlotbc.T_CLIENT         ,
                     T_CONTRACT        = v_nptxlotbc.T_CONTRACT       ,
                     T_FIID            = v_nptxlotbc.T_FIID           ,
                     T_KIND            = v_nptxlotbc.T_KIND           ,
                     T_TYPE            = v_nptxlotbc.T_TYPE           ,
                     T_BUYID           = v_nptxlotbc.T_BUYID          ,
                     T_REALID          = v_nptxlotbc.T_REALID         ,
                     T_AMOUNT          = v_nptxlotbc.T_AMOUNT         ,
                     T_SALE            = v_nptxlotbc.T_SALE           ,
                     T_VIRGIN          = v_nptxlotbc.T_VIRGIN         ,
                     T_COMPAMOUNT      = v_nptxlotbc.T_COMPAMOUNT     ,
                     T_PRICE           = v_nptxlotbc.T_PRICE          ,
                     T_PRICEFIID       = v_nptxlotbc.T_PRICEFIID      ,
                     T_TOTALCOST       = v_nptxlotbc.T_TOTALCOST      ,
                     T_NKD             = v_nptxlotbc.T_NKD            ,
                     T_BUYDATE         = v_nptxlotbc.T_BUYDATE        ,
                     T_SALEDATE        = v_nptxlotbc.T_SALEDATE       ,
                     T_SORTCODE        = v_nptxlotbc.T_SORTCODE       ,
                     T_RETFLAG         = v_nptxlotbc.T_RETFLAG        ,
                     T_ISFREE          = v_nptxlotbc.T_ISFREE         ,
                     T_BEGLOTID        = v_nptxlotbc.T_BEGLOTID       ,
                     T_CHILDID         = v_nptxlotbc.T_CHILDID        ,
                     T_BEGBUYDATE      = v_nptxlotbc.T_BEGBUYDATE     ,
                     T_BEGSALEDATE     = v_nptxlotbc.T_BEGSALEDATE    ,
                     T_INACC           = v_nptxlotbc.T_INACC          ,
                     T_BLOCKED         = v_nptxlotbc.T_BLOCKED        ,
                     T_ORIGIN          = v_nptxlotbc.T_ORIGIN         ,
                     T_GOID            = v_nptxlotbc.T_GOID           ,
                     T_CLGOID          = v_nptxlotbc.T_CLGOID         ,
                     T_OLDDATE         = v_nptxlotbc.T_OLDDATE        ,
                     T_ORDFORSALE      = v_nptxlotbc.T_ORDFORSALE     ,
                     T_ORDFORREPO      = v_nptxlotbc.T_ORDFORREPO     ,
                     T_RQID            = v_nptxlotbc.T_RQID           ,
                     T_NOTCOUNTEDONIIS = v_nptxlotbc.T_NOTCOUNTEDONIIS
               WHERE T_ID = one_nptxbc.t_ObjID;

              DELETE FROM DNPTXLOTBC_DBT WHERE t_LotBCID = one_nptxbc.t_BackObjID; 

            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LNK THEN 
            BEGIN
              SELECT * INTO v_nptxlnkbc
                FROM DNPTXLNKBC_DBT
               WHERE t_LnkBCID = one_nptxbc.t_BackObjID;

              
              UPDATE DNPTXLNK_DBT
                 SET T_CLIENT     = v_nptxlnkbc.T_CLIENT    , 
                     T_CONTRACT   = v_nptxlnkbc.T_CONTRACT  , 
                     T_FIID       = v_nptxlnkbc.T_FIID      , 
                     T_BUYID      = v_nptxlnkbc.T_BUYID     , 
                     T_SALEID     = v_nptxlnkbc.T_SALEID    , 
                     T_SOURCEID   = v_nptxlnkbc.T_SOURCEID  , 
                     T_TYPE       = v_nptxlnkbc.T_TYPE      , 
                     T_DATE       = v_nptxlnkbc.T_DATE      , 
                     T_AMOUNT     = v_nptxlnkbc.T_AMOUNT    , 
                     T_SHORT      = v_nptxlnkbc.T_SHORT     , 
                     T_VIRGIN     = v_nptxlnkbc.T_VIRGIN    , 
                     T_RETFLAG    = v_nptxlnkbc.T_RETFLAG   , 
                     T_PRIVAMOUNT = v_nptxlnkbc.T_PRIVAMOUNT
               WHERE T_ID = one_nptxbc.t_ObjID;        

              DELETE FROM DNPTXLNKBC_DBT WHERE t_LnkBCID = one_nptxbc.t_BackObjID;
            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LS THEN
            BEGIN
              SELECT * INTO v_nptxlsbc
                FROM DNPTXLSBC_DBT
               WHERE t_LsBCID = one_nptxbc.t_BackObjID;

              UPDATE DNPTXLS_DBT
                 SET T_CHILDID  = v_nptxlsbc.T_CHILDID , 
                     T_PARENTID = v_nptxlsbc.T_PARENTID, 
                     T_SHORT    = v_nptxlsbc.T_SHORT
               WHERE T_ID = one_nptxbc.t_ObjID;       

              DELETE FROM DNPTXLSBC_DBT WHERE t_LsBCID = one_nptxbc.t_BackObjID;
            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_TS THEN
            BEGIN
              SELECT * INTO v_nptxtsbc
                FROM DNPTXTSBC_DBT
               WHERE t_TsBCID = one_nptxbc.t_BackObjID;

              UPDATE DNPTXTS_DBT
                 SET T_CLIENT   = v_nptxtsbc.T_CLIENT  , 
                     T_CONTRACT = v_nptxtsbc.T_CONTRACT, 
                     T_FIID     = v_nptxtsbc.T_FIID    , 
                     T_BUYID    = v_nptxtsbc.T_BUYID   , 
                     T_SALEID   = v_nptxtsbc.T_SALEID  , 
                     T_TYPE     = v_nptxtsbc.T_TYPE    , 
                     T_BEGDATE  = v_nptxtsbc.T_BEGDATE , 
                     T_ENDDATE  = v_nptxtsbc.T_ENDDATE , 
                     T_AMOUNT   = v_nptxtsbc.T_AMOUNT 
               WHERE T_ID = one_nptxbc.t_ObjID;       

              DELETE FROM DNPTXTSBC_DBT WHERE t_TsBCID = one_nptxbc.t_BackObjID;
            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_GO THEN
            BEGIN
              SELECT * INTO v_nptxgobc
                FROM DNPTXGOBC_DBT
               WHERE t_GoBCID = one_nptxbc.t_BackObjID;

              UPDATE DNPTXGO_DBT
                 SET T_DOCKIND      = v_nptxgobc.T_DOCKIND     , 
                     T_DOCUMENTID   = v_nptxgobc.T_DOCUMENTID  , 
                     T_KIND         = v_nptxgobc.T_KIND        , 
                     T_CODE         = v_nptxgobc.T_CODE        , 
                     T_SALEDATE     = v_nptxgobc.T_SALEDATE    , 
                     T_BUYDATE      = v_nptxgobc.T_BUYDATE     , 
                     T_FIID         = v_nptxgobc.T_FIID        , 
                     T_OLDFACEVALUE = v_nptxgobc.T_OLDFACEVALUE, 
                     T_NEWFACEVALUE = v_nptxgobc.T_NEWFACEVALUE 
               WHERE T_ID = one_nptxbc.t_ObjID;       

              DELETE FROM DNPTXGOBC_DBT WHERE t_GoBCID = one_nptxbc.t_BackObjID;
            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_GOFI THEN
            BEGIN
              SELECT * INTO v_nptxgofibc
                FROM DNPTXGOFIBC_DBT
               WHERE t_GoFiBCID = one_nptxbc.t_BackObjID;

              UPDATE DNPTXGOFI_DBT
                 SET T_GOID        = v_nptxgofibc.T_GOID       , 
                     T_NUM         = v_nptxgofibc.T_NUM        , 
                     T_NEWFIID     = v_nptxgofibc.T_NEWFIID    , 
                     T_NUMERATOR   = v_nptxgofibc.T_NUMERATOR  , 
                     T_DENOMINATOR = v_nptxgofibc.T_DENOMINATOR 
               WHERE T_ID = one_nptxbc.t_ObjID;       

              DELETE FROM DNPTXGOFIBC_DBT WHERE t_GoFiBCID = one_nptxbc.t_BackObjID;
            END;
          END IF;

        ELSIF one_nptxbc.t_Action = RSI_NPTXC.NPTXBC_ACTION_DELETE THEN

          IF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LOT THEN
            BEGIN
              INSERT INTO DNPTXLOT_DBT (T_ID             ,
                                        T_DOCKIND        ,
                                        T_DOCID          ,
                                        T_DEALDATE       ,
                                        T_DEALTIME       ,
                                        T_DEALCODE       ,
                                        T_DEALCODETS     ,
                                        T_CLIENT         ,
                                        T_CONTRACT       ,
                                        T_FIID           ,
                                        T_KIND           ,
                                        T_TYPE           ,
                                        T_BUYID          ,
                                        T_REALID         ,
                                        T_AMOUNT         ,
                                        T_SALE           ,
                                        T_VIRGIN         ,
                                        T_COMPAMOUNT     ,
                                        T_PRICE          ,
                                        T_PRICEFIID      ,
                                        T_TOTALCOST      ,
                                        T_NKD            ,
                                        T_BUYDATE        ,
                                        T_SALEDATE       ,
                                        T_SORTCODE       ,
                                        T_RETFLAG        ,
                                        T_ISFREE         ,
                                        T_BEGLOTID       ,
                                        T_CHILDID        ,
                                        T_BEGBUYDATE     ,
                                        T_BEGSALEDATE    ,
                                        T_INACC          ,
                                        T_BLOCKED        ,
                                        T_ORIGIN         ,
                                        T_GOID           ,
                                        T_CLGOID         ,
                                        T_OLDDATE        ,
                                        T_ORDFORSALE     ,
                                        T_ORDFORREPO     ,
                                        T_RQID           ,
                                        T_NOTCOUNTEDONIIS)
                 SELECT LotBc.T_ID             ,
                        LotBc.T_DOCKIND        ,
                        LotBc.T_DOCID          ,
                        LotBc.T_DEALDATE       ,
                        LotBc.T_DEALTIME       ,
                        LotBc.T_DEALCODE       ,
                        LotBc.T_DEALCODETS     ,
                        LotBc.T_CLIENT         ,
                        LotBc.T_CONTRACT       ,
                        LotBc.T_FIID           ,
                        LotBc.T_KIND           ,
                        LotBc.T_TYPE           ,
                        LotBc.T_BUYID          ,
                        LotBc.T_REALID         ,
                        LotBc.T_AMOUNT         ,
                        LotBc.T_SALE           ,
                        LotBc.T_VIRGIN         ,
                        LotBc.T_COMPAMOUNT     ,
                        LotBc.T_PRICE          ,
                        LotBc.T_PRICEFIID      ,
                        LotBc.T_TOTALCOST      ,
                        LotBc.T_NKD            ,
                        LotBc.T_BUYDATE        ,
                        LotBc.T_SALEDATE       ,
                        LotBc.T_SORTCODE       ,
                        LotBc.T_RETFLAG        ,
                        LotBc.T_ISFREE         ,
                        LotBc.T_BEGLOTID       ,
                        LotBc.T_CHILDID        ,
                        LotBc.T_BEGBUYDATE     ,
                        LotBc.T_BEGSALEDATE    ,
                        LotBc.T_INACC          ,
                        LotBc.T_BLOCKED        ,
                        LotBc.T_ORIGIN         ,
                        LotBc.T_GOID           ,
                        LotBc.T_CLGOID         ,
                        LotBc.T_OLDDATE        ,
                        LotBc.T_ORDFORSALE     ,
                        LotBc.T_ORDFORREPO     ,
                        LotBc.T_RQID           ,
                        LotBc.T_NOTCOUNTEDONIIS
                   FROM DNPTXLOTBC_DBT LotBc                              
                 WHERE LotBc.t_LotBCID = one_nptxbc.t_BackObjID;

              DELETE FROM DNPTXLOTBC_DBT WHERE t_LotBCID = one_nptxbc.t_BackObjID;
            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LNK THEN 
            BEGIN
              INSERT INTO DNPTXLNK_DBT (T_ID        ,
                                        T_CLIENT    ,
                                        T_CONTRACT  ,
                                        T_FIID      ,
                                        T_BUYID     ,
                                        T_SALEID    ,
                                        T_SOURCEID  ,
                                        T_TYPE      ,
                                        T_DATE      ,
                                        T_AMOUNT    ,
                                        T_SHORT     ,
                                        T_VIRGIN    ,
                                        T_RETFLAG   ,
                                        T_PRIVAMOUNT)
                 SELECT LnkBc.T_ID        ,
                        LnkBc.T_CLIENT    ,
                        LnkBc.T_CONTRACT  ,
                        LnkBc.T_FIID      ,
                        LnkBc.T_BUYID     ,
                        LnkBc.T_SALEID    ,
                        LnkBc.T_SOURCEID  ,
                        LnkBc.T_TYPE      ,
                        LnkBc.T_DATE      ,
                        LnkBc.T_AMOUNT    ,
                        LnkBc.T_SHORT     ,
                        LnkBc.T_VIRGIN    ,
                        LnkBc.T_RETFLAG   ,
                        LnkBc.T_PRIVAMOUNT
                   FROM DNPTXLNKBC_DBT LnkBc                              
                 WHERE LnkBc.t_LnkBCID = one_nptxbc.t_BackObjID;

              DELETE FROM DNPTXLNKBC_DBT WHERE t_LnkBCID = one_nptxbc.t_BackObjID;
            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_LS THEN
            BEGIN
              INSERT INTO DNPTXLS_DBT (T_ID      ,
                                       T_CHILDID ,
                                       T_PARENTID,
                                       T_SHORT)
                 SELECT LsBc.T_ID      ,
                        LsBc.T_CHILDID ,
                        LsBc.T_PARENTID,
                        LsBc.T_SHORT   
                   FROM DNPTXLSBC_DBT LsBc                              
                 WHERE LsBc.t_LsBCID = one_nptxbc.t_BackObjID;

              DELETE FROM DNPTXLSBC_DBT WHERE t_LsBCID = one_nptxbc.t_BackObjID;
            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_TS THEN
            BEGIN
              INSERT INTO DNPTXTS_DBT (T_ID      ,
                                       T_CLIENT  ,
                                       T_CONTRACT,
                                       T_FIID    ,
                                       T_BUYID   ,
                                       T_SALEID  ,
                                       T_TYPE    ,
                                       T_BEGDATE ,
                                       T_ENDDATE ,
                                       T_AMOUNT)
                 SELECT TsBc.T_ID      ,
                        TsBc.T_CLIENT  ,
                        TsBc.T_CONTRACT,
                        TsBc.T_FIID    ,
                        TsBc.T_BUYID   ,
                        TsBc.T_SALEID  ,
                        TsBc.T_TYPE    ,
                        TsBc.T_BEGDATE ,
                        TsBc.T_ENDDATE ,
                        TsBc.T_AMOUNT  
                   FROM DNPTXTSBC_DBT TsBc                              
                 WHERE TsBc.t_TsBCID = one_nptxbc.t_BackObjID;
                 
              DELETE FROM DNPTXTSBC_DBT WHERE t_TsBCID = one_nptxbc.t_BackObjID;


            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_GO THEN
            BEGIN
              INSERT INTO DNPTXGO_DBT (T_ID          ,
                                       T_DOCKIND     ,
                                       T_DOCUMENTID  ,
                                       T_KIND        ,
                                       T_CODE        ,
                                       T_SALEDATE    ,
                                       T_BUYDATE     ,
                                       T_FIID        ,
                                       T_OLDFACEVALUE,
                                       T_NEWFACEVALUE)
                 SELECT GoBc.T_ID          ,
                        GoBc.T_DOCKIND     ,
                        GoBc.T_DOCUMENTID  ,
                        GoBc.T_KIND        ,
                        GoBc.T_CODE        ,
                        GoBc.T_SALEDATE    ,
                        GoBc.T_BUYDATE     ,
                        GoBc.T_FIID        ,
                        GoBc.T_OLDFACEVALUE,
                        GoBc.T_NEWFACEVALUE
                   FROM DNPTXGOBC_DBT GoBc                              
                 WHERE GoBc.t_GoBCID = one_nptxbc.t_BackObjID;
                 
              DELETE FROM DNPTXGOBC_DBT WHERE t_GoBCID = one_nptxbc.t_BackObjID;
            END;
          ELSIF one_nptxbc.t_ObjKind = RSI_NPTXC.NPTXBC_OBJKIND_GOFI THEN
            BEGIN
              INSERT INTO DNPTXGOFI_DBT (T_ID         ,
                                         T_GOID       ,
                                         T_NUM        ,
                                         T_NEWFIID    ,
                                         T_NUMERATOR  ,
                                         T_DENOMINATOR)
                 SELECT GoFiBc.T_ID         ,
                        GoFiBc.T_GOID       ,
                        GoFiBc.T_NUM        ,
                        GoFiBc.T_NEWFIID    ,
                        GoFiBc.T_NUMERATOR  ,
                        GoFiBc.T_DENOMINATOR
                   FROM DNPTXGOFIBC_DBT GoFiBc                              
                 WHERE GoFiBc.t_GoFiBCID = one_nptxbc.t_BackObjID;
                 
              DELETE FROM DNPTXGOFIBC_DBT WHERE t_GoFiBCID = one_nptxbc.t_BackObjID;
            END;
          END IF;

        END IF;

      END LOOP;

      DELETE FROM DNPTXBC_DBT WHERE t_ID_Operation = pID_Operation AND t_ID_Step = pID_Step;

      RSI_TRG_DNPTXLOT_DBT.v_IsRestoreBC  := FALSE;

    END; --RestoreNPTX

    -- Процедура отката формирования регистров НДФЛ
    PROCEDURE RecoilCreateLots( pOperDate      IN DATE, 
                                pClient        IN NUMBER,
                                pIIS           IN CHAR DEFAULT CHR(0), 
                                pFIID          IN NUMBER DEFAULT -1,
                                pRecalc        IN CHAR DEFAULT CHR(0),
                                pID_Operation  IN NUMBER DEFAULT 0,
                                pID_Step       IN NUMBER DEFAULT 0,
                                pContract      IN NUMBER DEFAULT 0
                              )  
    IS
       v_BegDate    DATE;
       v_IIS        NUMBER;
       v_ExistsCalc NUMBER := 0;
    BEGIN
       v_IIS := iif((pIIS = CHR(0)), 0, 1);

       IF (pRecalc = CHR(0)) THEN
         RSI_NPTO.RecoilCalcPeriodDate(RSI_NPTXC.NPTXCALC_CALCLINKS, pClient, pOperDate, pIIS, 0, pContract);
       END IF;
       v_BegDate := RSI_NPTO.GetCalcPeriodDate(RSI_NPTXC.NPTXCALC_CALCLINKS, pClient, pIIS, 0, pContract)+1;

       IF (v_BegDate <= RSI_NPTO.GetCalcPeriodDate(RSI_NPTXC.NPTXCALC_CALCNDFL, pClient, pIIS, 0, pContract)) THEN
          RSI_NPTMSG.PutMsg( RSI_NPTXC.MES_ERROR, 'Попытка отката расчета связей НДФЛ в периоде, по которому были рассчитаны налоги' );
          RSI_NPTO.SetError( RSI_NPTXC.NPTX_ERROR_20625,''); 
       END IF;

       IF pID_Operation = 0 THEN
         RecoilInsertTS(v_BegDate, pClient, v_IIS, pFIID, pContract);
         RecoilInsertLinks(v_BegDate, pClient, v_IIS, pFIID, pContract);
         RecoilInsertLots(v_BegDate, pClient, v_IIS, pFIID, pContract);
         RecoilInsertGO(pOperDate, v_BegDate, pClient, v_IIS, pFIID, pContract );
       ELSE
         RestoreNPTX(pID_Operation, pID_Step);
       END IF;
    END;

    -- Процедура отката предыдущих расчетов без сохранения
    PROCEDURE DeletePrevCalc(pDocID         IN NUMBER,
                             pClient        IN NUMBER,
                             pIIS           IN NUMBER DEFAULT 0, 
                             pFIID          IN NUMBER DEFAULT -1,
                             pID_Operation  IN NUMBER DEFAULT 0,
                             pID_Step       IN NUMBER DEFAULT 0,
                             pContract      IN NUMBER DEFAULT 0
                            )
    IS
       v_nptxop   dnptxop_dbt%ROWTYPE;
       v_BegDate  DATE;
       v_EndDate  DATE;
    BEGIN

       RSI_TRG_DNPTXLOT_DBT.v_ID_Operation := pID_Operation;
       RSI_TRG_DNPTXLOT_DBT.v_ID_Step      := pID_Step;

       select * into v_nptxop
         from dnptxop_dbt
        where t_ID = pDocID;

       if( v_nptxop.t_Recalc = 'X' )then
          v_BegDate := v_nptxop.t_BegRecalcDate;
          v_EndDate := v_nptxop.t_EndRecalcDate;
       else
          -- при расчете (без пересчета) всегда делаем откат ЗА дату последней операции
          v_BegDate := RSI_NPTO.GetCalcPeriodDate(RSI_NPTXC.NPTXCALC_CALCLINKS, pClient, iif(pIIS=1,'X',chr(0)), 0, pContract);
          v_EndDate := TO_DATE('31.12.9999','DD.MM.YYYY');
       end if;

       if( v_nptxop.t_PrevDate != TO_DATE('01.01.0001','DD.MM.YYYY') and v_nptxop.t_PrevDate != TO_DATE('31.12.'||TO_CHAR(TO_NUMBER(TO_CHAR(v_nptxop.t_OperDate,'YYYY'))-1),'DD.MM.YYYY') )then
          if( v_nptxop.t_Recalc != 'X' or (v_nptxop.t_Recalc = 'X' and v_nptxop.t_CalcNDFL = 'X') )then
             RecoilInsertTS(v_BegDate, pClient, pIIS, pFIID, pContract);
             RecoilInsertLinks(v_BegDate, pClient, pIIS, pFIID, pContract);
             RecoilInsertLots(v_BegDate, pClient, pIIS, pFIID, pContract);
             RecoilInsertGO(v_EndDate, v_BegDate, pClient, pIIS, pFIID, pContract );
          end if;

       end if;

       RSI_TRG_DNPTXLOT_DBT.v_ID_Operation := 0;
       RSI_TRG_DNPTXLOT_DBT.v_ID_Step      := 0;
    END; -- DeletePrevCalc


    PROCEDURE RecoilDeletePrevCalc(pDocID         IN NUMBER,
                                   pID_Operation  IN NUMBER DEFAULT 0,
                                   pID_Step       IN NUMBER DEFAULT 0)
    IS

    BEGIN

      IF pID_Operation > 0 THEN
        RestoreNPTX(pID_Operation, pID_Step);
      END IF;

    END; -- RecoilDeletePrevCalc

    --Выполняет обработку таблицы состояния (тип записей - "покупки") при создании/обновлении связи.
    --Используется из триггеров по таблице связей.
    PROCEDURE UpdateTSBuyByLink( pLinkType IN NUMBER, pBuyID IN NUMBER, pSaleID IN NUMBER, pLinkDate IN DATE, pEndDate IN DATE, pAmount IN NUMBER )
    IS
      CURSOR BuyTS IS
              SELECT TS.*
                FROM DNPTXTS_DBT TS
               WHERE TS.T_TYPE = RSI_NPTXC.NPTXTS_BUY
                 AND TS.T_BUYID = pBuyID
                 AND TS.T_ENDDATE = TO_DATE('01.01.0001','DD.MM.YYYY')
               ORDER BY TS.T_BEGDATE ASC;

       IsPart2PR NUMBER := 0;
       Rest NUMBER := pAmount;
       Q NUMBER := 0;
       v_Break BOOLEAN := FALSE;
       v_TS_ID NUMBER := 0;
    BEGIN

       IF( pLinkType = RSI_NPTXC.NPTXLNK_REPO or pLinkType = RSI_NPTXC.NPTXLNK_SUBSTREPO )THEN
          IsPart2PR := 1;
       END IF;

       FOR OneRec IN BuyTS LOOP

          Q := OneRec.t_Amount;

          if( Q > Rest )then

             update dnptxts_dbt
                set t_amount = t_amount - Rest
              where t_ID = OneRec.t_ID;

             if( v_TS_ID > 0 )then
                update dnptxts_dbt
                   set t_amount = t_amount + Rest
                where t_id = v_TS_ID;
             else

                BEGIN
                  SELECT t_ID
                    INTO v_TS_ID
                    FROM dnptxts_dbt
                   WHERE T_TYPE     = RSI_NPTXC.NPTXTS_BUY
                     AND T_BUYID    = pBuyID
                     AND T_SALEID   = (case when IsPart2PR = 1 then 0 else pSaleID end)
                     AND T_BEGDATE  = (case when IsPart2PR = 1 then pEndDate else OneRec.t_BEGDATE end)
                     AND T_ENDDATE  = (case when IsPart2PR = 1 then TO_DATE('01.01.0001','DD.MM.YYYY') else pLinkDate end);

                  UPDATE dnptxts_dbt
                     SET t_Amount = t_Amount + Rest
                   WHERE t_ID = v_TS_ID;

                EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                    BEGIN

                      INSERT INTO dnptxts_dbt( T_CLIENT   ,
                                               T_CONTRACT ,
                                               T_FIID     ,
                                               T_BUYID    ,
                                               T_SALEID   ,
                                               T_TYPE     ,
                                               T_BEGDATE  ,
                                               T_ENDDATE  ,
                                               T_AMOUNT
                                             )
                                       VALUES( OneRec.t_Client,                                                                     --T_CLIENT
                                               OneRec.t_Contract,                                                                   --T_CONTRACT
                                               OneRec.t_FIID,                                                                       --T_FIID
                                               pBuyID,                                                                              --T_BUYID
                                               case when IsPart2PR = 1 then 0 else pSaleID end,                                    --T_SALEID
                                               RSI_NPTXC.NPTXTS_BUY,                                                                --T_TYPE
                                               case when IsPart2PR = 1 then pEndDate else OneRec.t_BEGDATE end,                    --T_BEGDATE
                                               case when IsPart2PR = 1 then TO_DATE('01.01.0001','DD.MM.YYYY') else pLinkDate end, --T_ENDDATE
                                               Rest                                                                                 --T_AMOUNT
                                             ) RETURNING t_ID INTO v_TS_ID;
                    END;
                END;

                if( IsPart2PR = 0 )then
                   v_TS_ID := 0;
                end if;

             end if;

             v_Break := TRUE;

          elsif( Q = Rest )then

             Q := Rest;

             if( IsPart2PR = 1 ) then

                update dnptxts_dbt
                   set t_EndDate = pEndDate
                 where t_ID = OneRec.t_ID;
               
                if( v_TS_ID > 0 )then
                   update dnptxts_dbt
                      set t_amount = t_amount + Rest
                   where t_id = v_TS_ID;
                else

                   BEGIN
                     SELECT t_ID
                       INTO v_TS_ID
                       FROM dnptxts_dbt
                      WHERE T_TYPE     = RSI_NPTXC.NPTXTS_BUY
                        AND T_BUYID    = pBuyID
                        AND T_SALEID   = 0
                        AND T_BEGDATE  = pEndDate
                        AND T_ENDDATE  = TO_DATE('01.01.0001','DD.MM.YYYY');

                     UPDATE dnptxts_dbt
                        SET t_Amount = t_Amount + Rest
                      WHERE t_ID = v_TS_ID;

                   EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                       BEGIN

                         INSERT INTO dnptxts_dbt( T_CLIENT   ,
                                                  T_CONTRACT ,
                                                  T_FIID     ,
                                                  T_BUYID    ,
                                                  T_SALEID   ,
                                                  T_TYPE     ,
                                                  T_BEGDATE  ,
                                                  T_ENDDATE  ,
                                                  T_AMOUNT
                                                )
                                          VALUES( OneRec.t_Client,                    --T_CLIENT
                                                  OneRec.t_Contract,                  --T_CONTRACT
                                                  OneRec.t_FIID,                      --T_FIID
                                                  pBuyID,                             --T_BUYID
                                                  0,                                  --T_SALEID
                                                  RSI_NPTXC.NPTXTS_BUY,               --T_TYPE
                                                  pEndDate,                           --T_BEGDATE
                                                  TO_DATE('01.01.0001','DD.MM.YYYY'), --T_ENDDATE
                                                  Rest                                --T_AMOUNT
                                                ) RETURNING t_ID INTO v_TS_ID;
                       END;
                   END;
                end if;
             else
                update dnptxts_dbt
                   set t_EndDate = pLinkDate,
                       t_SaleID = pSaleID
                 where t_ID = OneRec.t_ID;
             end if;

             v_Break := TRUE;

          else--Q < Rest

             if( IsPart2PR = 1 ) then

                update dnptxts_dbt
                   set t_EndDate = pEndDate
                 where t_ID = OneRec.t_ID;
               
                if( v_TS_ID > 0 )then
                   update dnptxts_dbt
                      set t_amount = t_amount + Rest
                   where t_id = v_TS_ID;
                else
                   BEGIN
                     SELECT t_ID
                       INTO v_TS_ID
                       FROM dnptxts_dbt
                      WHERE T_TYPE     = RSI_NPTXC.NPTXTS_BUY
                        AND T_BUYID    = pBuyID
                        AND T_SALEID   = 0
                        AND T_BEGDATE  = pEndDate
                        AND T_ENDDATE  = TO_DATE('01.01.0001','DD.MM.YYYY');

                     UPDATE dnptxts_dbt
                        SET t_Amount = t_Amount + Q
                      WHERE t_ID = v_TS_ID;

                   EXCEPTION
                     WHEN NO_DATA_FOUND THEN
                       BEGIN

                         INSERT INTO dnptxts_dbt( T_CLIENT   ,
                                                  T_CONTRACT ,
                                                  T_FIID     ,
                                                  T_BUYID    ,
                                                  T_SALEID   ,
                                                  T_TYPE     ,
                                                  T_BEGDATE  ,
                                                  T_ENDDATE  ,
                                                  T_AMOUNT
                                                )
                                          VALUES( OneRec.t_Client,                    --T_CLIENT
                                                  OneRec.t_Contract,                  --T_CONTRACT
                                                  OneRec.t_FIID,                      --T_FIID
                                                  pBuyID,                             --T_BUYID
                                                  0,                                  --T_SALEID
                                                  RSI_NPTXC.NPTXTS_BUY,               --T_TYPE
                                                  pEndDate,                           --T_BEGDATE
                                                  TO_DATE('01.01.0001','DD.MM.YYYY'), --T_ENDDATE
                                                  Q                                   --T_AMOUNT
                                                ) RETURNING t_ID INTO v_TS_ID;
                       END;
                   END;
                end if;

             else
                update dnptxts_dbt
                   set t_EndDate = pLinkDate,
                       t_SaleID = pSaleID
                 where t_ID = OneRec.t_ID;
             end if;

             Rest := Rest - Q;

          end if;

          EXIT WHEN v_Break;

       END LOOP;

    END;--UpdateTSBuyByLink

    PROCEDURE ReCalcPrivAmountByLink( pBegDate IN DATE, pEndDate IN DATE, pClient IN NUMBER, pIIS IN NUMBER DEFAULT 0, pFIID IN NUMBER DEFAULT -1 )  
    IS
    BEGIN
       GetSettingsTax;
       if( ReestrValue.W3 = TRUE ) THEN
          update dnptxlnk_dbt LNK                                                                            
             SET LNK.T_PRIVAMOUNT = NVL((select sum(TS.T_AMOUNT) Amount                                         
                                         from dnptxts_dbt ts                                                  
                                        where TS.T_TYPE = RSI_NPTXC.NPTXTS_BUY                                                   
                                          and TS.T_BUYID = LNK.T_BUYID                                                 
                                          and TS.T_SALEID = LNK.T_SALEID                                                 
                                          and ( (rsi_npto.IsFavourIncome_NPTX(TS.T_ENDDATE,TS.T_BEGDATE,TS.T_FIID) in(1,2)
                                                  and ((MONTHS_BETWEEN(TS.T_ENDDATE,TS.T_BEGDATE)/12) > 5) ) or
                                                (rsi_npto.IsFavourIncome_NPTX(TS.T_ENDDATE,TS.T_BEGDATE,TS.T_FIID) in(3,4)
                                                  and ((MONTHS_BETWEEN(TS.T_ENDDATE,TS.T_BEGDATE)/12) > 1) )
                                              )
                                        ),0)
          where LNK.T_TYPE = RSI_NPTXC.NPTXLNK_DELIVER                                                                               
            and LNK.T_CLIENT = pClient                                                                            
            and RSI_NPTO.CheckContrIIS(LNK.t_Contract) = pIIS
            and LNK.T_FIID = case when pFIID != -1 then pFIID else LNK.T_FIID end                                         
            and LNK.T_DATE >= pBegDate                                               
            and LNK.T_DATE <= pEndDate;                                              
       else
          update dnptxlnk_dbt LNK                                                                          
             SET LNK.T_PRIVAMOUNT = NVL((select L.T_AMOUNT                                                    
                                           from v_npx_dnptxlot LB, v_npx_dnptxlot LS, dnptxlnk_dbt L          
                                          where L.T_ID = LNK.T_ID                                             
                                            and LB.T_ID = L.T_BUYID                                           
                                            and LS.T_ID = L.T_SALEID                                          
                                            and ( (rsi_npto.IsFavourIncome_NPTX(LS.T_SALEDATE,LB.T_BUYDATE,L.T_FIID) in(1,2)
                                                    and ((MONTHS_BETWEEN(LS.T_SALEDATE,LB.T_BUYDATE)/12) > 5) ) or
                                                  (rsi_npto.IsFavourIncome_NPTX(LS.T_SALEDATE,LB.T_BUYDATE,L.T_FIID) in(3,4)
                                                    and ((MONTHS_BETWEEN(LS.T_SALEDATE,LB.T_BUYDATE)/12) > 1) )
                                                )
                                        ),0)
          where LNK.T_TYPE = RSI_NPTXC.NPTXLNK_DELIVER                                                                             
            and LNK.T_CLIENT = pClient                                                                          
            and RSI_NPTO.CheckContrIIS(LNK.t_Contract) = pIIS                                                   
            and LNK.T_FIID = case when pFIID != -1 then pFIID else LNK.T_FIID end                                         
            and LNK.T_DATE >= pBegDate                                             
            and LNK.T_DATE <= pEndDate;                                            
       end if;
    END;--ReCalcPrivAmount

    FUNCTION GetPrivAmountByTS( pID IN NUMBER, pCalcDate IN DATE, pDDS IN DATE )  
       RETURN NUMBER DETERMINISTIC
    IS
       vAmount NUMBER := 0;
    BEGIN
       GetSettingsTax;
       if( ReestrValue.W3 = TRUE ) THEN
          select least( NVL((select sum(ts.T_AMOUNT)                                                    
                               from dnptxts_dbt ts          
                              where ts.T_TYPE = RSI_NPTXC.NPTXTS_BUY                                             
                                and ts.T_BUYID = t.T_BUYID                                           
                                and (ts.T_ENDDATE >= pCalcDate or ts.T_ENDDATE = TO_DATE('01.01.0001','DD.MM.YYYY'))
                                and ts.T_BEGDATE < pCalcDate                                            
                                and rsi_npto.IsFavourIncome_NPTX(pCalcDate,ts.T_BEGDATE,ts.T_FIID) in(3,4)
                                and (MONTHS_BETWEEN(pCalcDate,ts.T_BEGDATE)/12) > 1
                            ),0),
                        t.T_AMOUNT 
                       ) into vAmount
            from dnptxts_dbt t
           where t.t_id = pID;                                                                             
       else
          begin
             select ts.T_AMOUNT into vAmount                                                   
               from v_npx_dnptxlot LB, dnptxts_dbt ts          
              where ts.T_ID = pID                                             
                and LB.T_ID = ts.T_BUYID                                           
                and rsi_npto.IsFavourIncome_NPTX(pDDS,LB.T_BUYDATE,ts.T_FIID) in(3,4)
                and (MONTHS_BETWEEN(pDDS,LB.T_BUYDATE)/12) > 1;
          exception
            when NO_DATA_FOUND then
              vAmount := 0;
          end;
       end if;
       return vAmount;
    END;--GetPrivAmountByTS

    FUNCTION LISTAGG_DISTINCT(val IN VARCHAR2, sep IN VARCHAR2 DEFAULT ',')
      RETURN VARCHAR2
    AS
      ret_val VARCHAR2(4096) := '';
    BEGIN
      SELECT
        LISTAGG(tmp_val, '/') WITHIN GROUP (ORDER BY 1)
      INTO
        ret_val
      FROM
        (SELECT
           DISTINCT REGEXP_SUBSTR(val
                                 ,'[^' || sep || ']+'
                                 ,1
                                 ,LEVEL)
                      AS tmp_val
         FROM
           DUAL
         CONNECT BY
           REGEXP_SUBSTR(val
                        ,'[^' || sep || ']+'
                        ,1
                        ,LEVEL)
             IS NOT NULL);

      RETURN ret_val;
    END; --LISTAGG_DISTINCT

    FUNCTION ResidenceStatus(pClient IN NUMBER, pDate IN DATE)
      RETURN NUMBER
    AS
      vStatus NUMBER := 0;
    BEGIN
      vStatus :=
        GetFirstObjAttrNoDate(3, LPAD(pClient, 10, '0'), 42);-- DEF-78912 rsb_secur.GetMainObjAttrNoDate не корректно работает т.к. возвращает всегда 0 если категория менялась

      IF (vStatus = 0)
      THEN
        SELECT
          CASE WHEN party.t_NotResident = 'X' OR        --Стоит признак "Нерезидент"
                    party.t_IsResidenceUnknown = 'X' OR --или признак "Страна резидентности неизвестна"
                    ps.t_IsStateless = 'X' OR           --или признак "Лицо без гражданства"
                    party.t_NRCountry <> 'RUS'          --или гражданство не Россия
                    THEN 2 
               ELSE 1 END
        INTO
          vStatus
        FROM
          dparty_dbt party, (SELECT pClient AS t_PartyID FROM DUAL) q,
          dpersn_dbt ps
        WHERE party.t_PartyID(+) = q.t_PartyID
          AND ps.t_PersonID(+) = party.t_PartyID;
      END IF;

      RETURN vStatus;
    END; --ResidenceStatus
    
    FUNCTION CheckContrIISForNPTXWrtOff(pSubKind_Operation IN NUMBER, pContrID IN NUMBER)
       RETURN NUMBER
    AS
    BEGIN
       IF pSubKind_Operation = 20 AND RSI_NPTO.CheckContrIIS(pContrID) = 1
       THEN
          RETURN 1;
       END IF;
       
       RETURN 0;
    END;

    --Процедура определения налогового статуса клиента (ПОНСК)
    FUNCTION STB_IsResidentStatus(p_ClientID IN NUMBER, p_OnDate IN DATE) RETURN NUMBER
    AS
      v_ResidenceStatus NUMBER := 0;
    BEGIN

      v_ResidenceStatus := ResidenceStatus(p_ClientID, p_OnDate);

      IF v_ResidenceStatus = 1 OR v_ResidenceStatus = 7 THEN
        RETURN 1;
      END IF;

      RETURN 0;
    END;

    --Процедура определения налоговых ставок (ПОНС)
    PROCEDURE CreateTaxRatesBySNOB(p_ClientID IN NUMBER,
                                   p_TaxPeriod IN NUMBER,
                                   p_TaxBaseType IN NUMBER,
                                   p_TaxBase IN NUMBER,
                                   p_SNOB IN NUMBER,
                                   p_CalcDate IN DATE,
                                   p_SpecialTag IN VARCHAR2
                                  )
    AS
      v_IsResident CHAR(1) := CHR(0);
      v_RestTaxBase NUMBER := 0;

      v_rec DNPTXTAXBASERANGES_TMP%ROWTYPE;
    BEGIN

      DELETE FROM DNPTXTAXBASERANGES_TMP;

      IF STB_IsResidentStatus(p_ClientID, p_CalcDate) = 1 THEN
        v_IsResident := 'X';
      END IF;

      FOR one_typesnob IN (SELECT *
                             FROM DNPTXLINKSKINDSNOB_DBT
                            WHERE T_TAXBASETYPE = p_TaxBaseType
                              AND EXTRACT(YEAR FROM T_BEGDATETYPESNOB) <= p_TaxPeriod
                              AND EXTRACT(YEAR FROM T_ENDDATETYPESNOB) >= p_TaxPeriod
                          )
      LOOP
        v_RestTaxBase := p_TaxBase;

        FOR one_limit IN (SELECT LIM.*, LLV.T_NAME AS FullKBK,
                                 RANK() OVER(ORDER BY LIM.T_LIMITSNOB ASC) AS RowRange
                            FROM DNPTXSNOBLIMIT_DBT LIM, DLLVALUES_DBT LLV
                           WHERE LIM.T_TYPESNOB = one_typesnob.T_TYPESNOB
                             AND LIM.T_ISRESIDENT = v_IsResident
                             AND LLV.T_LIST = 3522
                             AND LLV.T_ELEMENT = LIM.T_CODEKBK
                           ORDER BY LIM.T_LIMITSNOB DESC
                         )
        LOOP

          v_rec.T_TAXBASETYPE := p_TaxBaseType;
          v_rec.T_TAXPERIOD   := p_TaxPeriod;
          v_rec.T_RANGE       := one_limit.RowRange;
          v_rec.T_TAXRATE     := one_limit.T_TAXRATE;
          v_rec.T_KBK         := one_limit.FullKBK;
          v_rec.T_TAXCALC     := 0;
          v_rec.T_TAXHOLD     := 0;
          v_rec.T_BASESUM     := 0;
          IF p_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK THEN
            v_rec.T_SPECIALTAG := p_SpecialTag;
          ELSE
            v_rec.T_SPECIALTAG := chr(0);
          END IF;

          IF v_RestTaxBase <> 0 AND one_limit.T_LIMITSNOB < (p_TaxBase + p_SNOB) THEN
            IF p_SNOB <= one_limit.T_LIMITSNOB THEN
              v_rec.T_BASESUM := v_RestTaxBase + p_SNOB - one_limit.T_LIMITSNOB;
            ELSE
              v_rec.T_BASESUM := v_RestTaxBase;
            END IF;

            v_RestTaxBase := v_RestTaxBase - v_rec.T_BASESUM;
          END IF;

          INSERT INTO DNPTXTAXBASERANGES_TMP VALUES v_rec;
        END LOOP;
      END LOOP;

    END;

    --Процедура Расчета Исчисленного Налога (ПРИН)
    PROCEDURE CalcTaxByRanges(p_ClientID IN NUMBER,
                              p_TaxPeriod IN NUMBER,
                              p_TaxBaseType IN NUMBER,
                              p_TaxBase IN NUMBER,
                              p_SNOB IN NUMBER,
                              p_CalcDate IN DATE,
                              p_SpecialTag IN VARCHAR2
                             )
    AS
      v_SumTaxBaseCurrPay NUMBER := 0;
      v_CalcPITax         NUMBER := 0;
    BEGIN
      CreateTaxRatesBySNOB(p_ClientID, p_TaxPeriod, p_TaxBaseType, p_TaxBase, p_SNOB, p_CalcDate, p_SpecialTag);

      FOR one_rec IN (SELECT *
                        FROM DNPTXTAXBASERANGES_TMP
                       ORDER BY T_RANGE ASC 
                     )
      LOOP

        SELECT NVL(SUM(t_TaxBaseCurrPay), 0), NVL(SUM(t_CalcPITax), 0)
          INTO v_SumTaxBaseCurrPay, v_CalcPITax
          FROM dnptxtotalbase_dbt 
         WHERE t_ClientID = p_ClientID
           AND t_StorState = RSI_NPTXC.NPTXTOTALBASE_STORSTATE_ACTIVE
           AND t_TaxBaseKind = p_TaxBaseType
           AND t_RateCalcPITax = one_rec.t_TaxRate
           AND t_TaxPeriod = p_TaxPeriod
           AND t_bcccalcpitax = one_rec.t_kbk;

        UPDATE DNPTXTAXBASERANGES_TMP
           SET T_TAXCALC = ROUND(((one_rec.t_BaseSum+v_SumTaxBaseCurrPay)*one_rec.t_TaxRate/100 - v_CalcPITax), 0)
         WHERE T_TAXBASETYPE = one_rec.t_TaxBaseType
           AND T_RANGE       = one_rec.t_Range
           AND T_TAXRATE     = one_rec.t_TaxRate
           AND t_kbk         = one_rec.t_kbk
           AND t_specialtag  = one_rec.t_specialtag;

      END LOOP;
    END;

    --Процедура расчета налога к удержанию (ПРасНаКу)
    PROCEDURE CalcHoldTaxByRanges(p_ClientID IN NUMBER,
                                  p_TaxPeriod IN NUMBER,
                                  p_TaxBaseType IN NUMBER,
                                  p_TaxBase IN NUMBER,
                                  p_SNOB IN NUMBER,
                                  p_CalcDate IN DATE,
                                  p_SpecialTag IN VARCHAR2
                                 )
    AS
      v_Kind    NUMBER := 0;
      v_TaxPaid NUMBER := 0;

      v_SumTaxBaseCurrPay NUMBER := 0;
      v_SumCalcPITax NUMBER := 0;
      v_SumHoldPITax NUMBER := 0;

      v_ObjKindCode VARCHAR2(20) := '';
      v_ObjKind NUMBER := 0;
    BEGIN
      CalcTaxByRanges(p_ClientID, p_TaxPeriod, p_TaxBaseType, p_TaxBase, p_SNOB, p_CalcDate, p_SpecialTag);

      FOR one_rec IN (SELECT *
                        FROM DNPTXTAXBASERANGES_TMP
                       ORDER BY T_RANGE ASC 
                     )
      LOOP
        v_ObjKind := 0;

        IF one_rec.t_KBK = RSI_NPTXC.NPTXKBK_MAIN THEN --КБК по основной ставке
          IF p_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB THEN
            v_ObjKind := RSI_NPTXC.TXOBJ_PAIDBILL;
          ELSIF p_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK THEN
            v_ObjKind := RSI_NPTXC.TXOBJ_PAIDGENERAL;
          ELSIF p_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS THEN
            v_ObjKind := RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS;
          END IF;
        ELSIF one_rec.t_KBK not in (RSI_NPTXC.NPTXKBK_MAIN, RSI_NPTXC.NPTXKBK_PROC) THEN --КБК по повышенной ставке
          v_ObjKindCode := 'PaidGeneral_'||TO_CHAR(one_Rec.t_TaxRate);
          
          IF p_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB THEN
            v_ObjKindCode := v_ObjKindCode||'_9';
          ELSIF p_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK THEN
            v_ObjKindCode := v_ObjKindCode||'_2';
          ELSIF p_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_IIS THEN
            v_ObjKindCode := v_ObjKindCode||'_ИИС';
          END IF;

          BEGIN
            SELECT t_Element INTO v_ObjKind FROM dnptxkind_dbt WHERE LOWER(t_Code) = LOWER(v_ObjKindCode);

            EXCEPTION
              WHEN NO_DATA_FOUND THEN v_ObjKind := 0;
          END;
        END IF; 

        IF v_ObjKind > 0 THEN

          SELECT NVL(SUM(t_TaxBaseCurrPay), 0), NVL(SUM(t_CalcPITax), 0), NVL(SUM(t_HoldPITax), 0)
            INTO v_SumTaxBaseCurrPay, v_SumCalcPITax, v_SumHoldPITax
            FROM dnptxtotalbase_dbt 
           WHERE t_ClientID = p_ClientID
             AND t_StorState = RSI_NPTXC.NPTXTOTALBASE_STORSTATE_ACTIVE
             AND t_TaxBaseKind = one_rec.t_TaxBaseType
             AND t_RateCalcPITax = one_rec.t_TaxRate
             AND t_TaxPeriod = p_TaxPeriod
             AND t_bcccalcpitax = one_rec.t_kbk
             AND 1 = CASE WHEN p_TaxBaseType = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK 
                     THEN CASE WHEN t_specialtag = one_rec.t_specialtag THEN 1 ELSE 0 END
                     ELSE 1 END;
          
          SELECT NVL(SUM(obj.t_Sum0), 0) INTO v_TaxPaid
            FROM dnptxobj_dbt obj
           WHERE obj.t_Client = p_CLientID
             AND obj.t_TaxPeriod = p_TaxPeriod
             AND obj.t_Kind = v_ObjKind;

          IF p_TaxPeriod >= GetTaxRegIntValue('COMMON\НДФЛ\НП_ВВОДА_ДОРАБОТОК_BOSS-2981', TRUNC(SYSDATE)) THEN
            UPDATE DNPTXTAXBASERANGES_TMP
               SET T_TAXHOLD     = (one_rec.t_taxcalc + v_SumCalcPITax) - v_SumHoldPITax
             WHERE T_TAXBASETYPE = one_rec.t_TaxBaseType
               AND T_RANGE       = one_rec.t_Range
               AND T_TAXRATE     = one_rec.t_TaxRate
               AND t_kbk         = one_rec.t_kbk
               AND t_specialtag  = one_rec.t_specialtag;
          ELSE
            IF one_rec.t_BaseSum = 0 THEN
              UPDATE DNPTXTAXBASERANGES_TMP
                 SET T_TAXHOLD     = v_SumCalcPITax - v_SumHoldPITax
               WHERE T_TAXBASETYPE = one_rec.t_TaxBaseType
                 AND T_RANGE       = one_rec.t_Range
                 AND T_TAXRATE     = one_rec.t_TaxRate
                 AND t_kbk         = one_rec.t_kbk;
            ELSE

              UPDATE DNPTXTAXBASERANGES_TMP
                 SET T_TAXHOLD = ROUND((one_rec.t_BaseSum+v_SumTaxBaseCurrPay)*one_rec.t_TaxRate/100 - v_TaxPaid, 0)
               WHERE T_TAXBASETYPE = one_rec.t_TaxBaseType
                 AND T_RANGE       = one_rec.t_Range
                 AND T_TAXRATE     = one_rec.t_TaxRate
                 AND t_kbk         = one_rec.t_kbk;
            END IF;
          END IF;
        END IF;
      END LOOP;
    END;

    FUNCTION GetTaxRegIntValue(p_KeyPath IN VARCHAR2, p_OnDate IN DATE ) RETURN NUMBER
    AS
      v_Val NUMBER;

      v_KeyID NUMBER;
    BEGIN

      v_KeyID := RSB_Common.RSI_GetRegParm(p_KeyPath);
      IF v_KeyID > 0 THEN

        BEGIN
          SELECT st.t_IntValue
            INTO v_Val
            FROM dregparm_dbt rg, dregparmtag_dbt tg, dregparmstory_dbt st 
           WHERE rg.t_KeyID = v_KeyID
             AND tg.t_KeyID = rg.t_KeyID 
             AND tg.t_Tag1 = 'TAX' 
             AND st.t_KeyID = tg.t_KeyID 
             AND st.t_DateBegin <= p_OnDate
             AND st.t_DateEnd >= p_OnDate;


          EXCEPTION
              WHEN NO_DATA_FOUND THEN v_Val := NULL;
        END;

        IF v_Val IS NULL THEN
          v_Val := Rsb_Common.GetRegIntValue(p_KeyPath, 0);
        END IF;

      END IF;

      RETURN v_Val;
    END;

    FUNCTION GetTaxRegDoubleValue(p_KeyPath IN VARCHAR2, p_OnDate IN DATE ) RETURN FLOAT
    AS
      v_Val FLOAT;

      v_KeyID NUMBER;
    BEGIN

      v_KeyID := RSB_Common.RSI_GetRegParm(p_KeyPath);
      IF v_KeyID > 0 THEN

        BEGIN
          SELECT st.t_DoubleValue
            INTO v_Val
            FROM dregparm_dbt rg, dregparmtag_dbt tg, dregparmstory_dbt st 
           WHERE rg.t_KeyID = v_KeyID
             AND tg.t_KeyID = rg.t_KeyID 
             AND tg.t_Tag1 = 'TAX' 
             AND st.t_KeyID = tg.t_KeyID 
             AND st.t_DateBegin <= p_OnDate
             AND st.t_DateEnd >= p_OnDate;


          EXCEPTION
              WHEN NO_DATA_FOUND THEN v_Val := NULL;
        END;

        IF v_Val IS NULL THEN
          v_Val := Rsb_Common.GetRegDoubleValue(p_KeyPath, 0);
        END IF;

      END IF;

      RETURN v_Val;
    END;

    FUNCTION GetTaxRegStringValue(p_KeyPath IN VARCHAR2, p_OnDate IN DATE ) RETURN VARCHAR2
    AS
      v_Val dregparmstory_dbt.t_StringValue%Type;

      v_KeyID NUMBER;
    BEGIN

      v_KeyID := RSB_Common.RSI_GetRegParm(p_KeyPath);
      IF v_KeyID > 0 THEN

        BEGIN
          SELECT st.t_StringValue
            INTO v_Val
            FROM dregparm_dbt rg, dregparmtag_dbt tg, dregparmstory_dbt st 
           WHERE rg.t_KeyID = v_KeyID
             AND tg.t_KeyID = rg.t_KeyID 
             AND tg.t_Tag1 = 'TAX' 
             AND st.t_KeyID = tg.t_KeyID 
             AND st.t_DateBegin <= p_OnDate
             AND st.t_DateEnd >= p_OnDate;


          EXCEPTION
              WHEN NO_DATA_FOUND THEN v_Val := NULL;
        END;

        IF v_Val IS NULL THEN
          v_Val := Rsb_Common.GetRegStrValue(p_KeyPath, 0);
        END IF;

      END IF;

      RETURN v_Val;
    END;

    FUNCTION GetTaxRegFlagValue(p_KeyPath IN VARCHAR2, p_OnDate IN DATE ) RETURN CHAR
    AS
      v_Val CHAR(1);

      v_KeyID NUMBER;
    BEGIN

      v_KeyID := RSB_Common.RSI_GetRegParm(p_KeyPath);
      IF v_KeyID > 0 THEN

        BEGIN
          SELECT st.t_BoolValue
            INTO v_Val
            FROM dregparm_dbt rg, dregparmtag_dbt tg, dregparmstory_dbt st 
           WHERE rg.t_KeyID = v_KeyID
             AND tg.t_KeyID = rg.t_KeyID 
             AND tg.t_Tag1 = 'TAX' 
             AND st.t_KeyID = tg.t_KeyID 
             AND st.t_DateBegin <= p_OnDate
             AND st.t_DateEnd >= p_OnDate;


          EXCEPTION
              WHEN NO_DATA_FOUND THEN v_Val := NULL;
        END;

        IF v_Val IS NULL THEN
          v_Val := Rsb_Common.GetRegFlagValue(p_KeyPath, 0);
        END IF;

      END IF;

      RETURN v_Val;
    END;

    FUNCTION GetTaxRegValueByType(p_KeyPath IN VARCHAR2, p_OnDate IN DATE, p_RegType IN INTEGER) RETURN VARCHAR2
    AS
      v_Val VARCHAR2(2000);
    BEGIN

      IF p_RegType = Rsb_Common.REG_TYPE_BINARY THEN
        v_Val := CHR(1);
      ELSE
        IF p_RegType = Rsb_Common.REG_TYPE_STRING THEN
          v_Val := GetTaxRegStringValue(p_KeyPath, p_OnDate);
        END IF;
      
        IF p_RegType = Rsb_Common.REG_TYPE_INT THEN
          v_Val := TO_CHAR(GetTaxRegIntValue(p_KeyPath, p_OnDate));
        END IF;
      
        IF p_RegType = Rsb_Common.REG_TYPE_DOUBLE THEN
          v_Val := TO_CHAR(GetTaxRegDoubleValue(p_KeyPath, p_OnDate));
        END IF;
      
        IF p_RegType = Rsb_Common.REG_TYPE_FLAG THEN
          v_Val := GetTaxRegFlagValue(p_KeyPath, p_OnDate);
          IF v_Val = CHR(0) THEN
            v_Val := 'NO';
          ELSE
            v_Val := 'YES';
          END IF;
        END IF;
      END IF;

      RETURN v_Val;
    END;

    PROCEDURE TaxeCalcFunctionPrm(pClientId IN NUMBER, pTaxPeriod IN NUMBER, pTaxBaseKind IN NUMBER, pSubKindOperation IN NUMBER)
    AS
      v_taxholdbrokdepo   NUMBER (32, 12);
      v_taxholdbroksofr   NUMBER (32, 12);
      v_taxholdbrok       NUMBER (32, 12);
      v_taxholdmaterial   NUMBER (32, 12);
      v_taxholdbill       NUMBER (32, 12);
      v_taxholdonb        NUMBER (32, 12);
    BEGIN
      EXECUTE IMMEDIATE 'truncate table dtaxcalctotal_tmp';

      EXECUTE IMMEDIATE 'truncate table dtaxecalcfunction_tmp';

      INSERT INTO dtaxecalcfunction_tmp (t_sys,
                                         t_taxbasekind,
                                         t_specialtag,
                                         t_taxbase,
                                         t_rate,
                                         t_codekbk,
                                         t_taxcalc,
                                         t_taxhold)
         (SELECT tb.t_syscome,
                 tb.t_taxbasekind,
                 tb.t_specialtag,
                 tb.t_taxbasecurrpay,
                 tb.t_rateholdpitax,
                 tb.t_bccholdpitax,
                 tb.t_calcpitax,
                 tb.t_holdpitax
            FROM dnptxtotalbase_dbt tb
           WHERE tb.t_storstate = RSI_NPTXC.NPTXTOTALBASE_STORSTATE_ACTIVE
             AND (tb.t_taxbasekind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB
                      OR tb.t_TaxBaseKind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK)
             AND tb.t_taxperiod = pTaxPeriod
             AND tb.t_clientid = pClientId);

      IF(pSubKindOperation = RSI_NPTXC.DL_TXHOLD_OPTYPE_ENDYEAR) THEN
        INSERT INTO dtaxecalcfunction_tmp (t_sys,
                                           t_taxbasekind,
                                           t_specialtag,
                                           t_taxbase,
                                           t_rate,
                                           t_codekbk,
                                           t_taxcalc,
                                           t_taxhold)
           (SELECT 2,
                   tb.t_taxbasekind,
                   CASE WHEN tb.t_taxbasekind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK THEN 'БРОК' ELSE CHR(1) END,
                   tb.t_nobamount,
                   tb.t_ndflkeeprate,
                   tb.t_kbkkeep,
                   tb.t_ndflcalcamount,
                   tb.t_ndflkeepamount
              FROM dnptxtbext_dbt tb
             WHERE tb.t_taxperiod = pTaxPeriod AND tb.t_clientid = pClientId);

        FOR one_rec IN (SELECT DISTINCT t_codekbk, t_rate FROM dtaxecalcfunction_tmp)
        LOOP
           SELECT ROUND (NVL (SUM (t_taxbase * t_rate / 100) - SUM (t_taxhold), 0))
             INTO v_taxholdbrokdepo
             FROM dtaxecalcfunction_tmp
            WHERE     t_codekbk = one_rec.t_codekbk
                AND t_sys = 2
                AND t_taxbasekind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK
                AND t_specialtag = 'БРОК';

           SELECT ROUND (NVL (SUM (t_taxbase * t_rate / 100) - SUM (t_taxhold), 0))
             INTO v_taxholdbrok
             FROM dtaxecalcfunction_tmp
            WHERE t_codekbk = one_rec.t_codekbk AND t_taxbasekind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK AND t_specialtag = 'БРОК';
    
           v_taxholdbroksofr := NVL (v_taxholdbrok - v_taxholdbrokdepo, 0);

           SELECT NVL (SUM (t_taxcalc) - SUM (t_taxhold), 0)
             INTO v_taxholdmaterial
             FROM dtaxecalcfunction_tmp
            WHERE t_codekbk = one_rec.t_codekbk AND t_taxbasekind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK AND t_specialtag = 'МАТ';

           SELECT NVL (SUM (t_taxcalc) - SUM (t_taxhold), 0)
             INTO v_taxholdbill
             FROM dtaxecalcfunction_tmp
            WHERE t_codekbk = one_rec.t_codekbk AND t_taxbasekind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK AND t_specialtag = 'ВЕКС';

           SELECT NVL (SUM (t_taxcalc) - SUM (t_taxhold), 0)
             INTO v_taxholdonb
             FROM dtaxecalcfunction_tmp
            WHERE t_codekbk = one_rec.t_codekbk AND t_taxbasekind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_ONB;

           INSERT INTO dtaxcalctotal_tmp (t_taxholdbrokdepo,
                                          t_taxholdbroksofr,
                                          t_taxholdbrok,
                                          t_taxholdmaterial,
                                          t_taxholdbill,
                                          t_taxholdonb,
                                          t_codekbk,
                                          t_rate)
                VALUES (v_taxholdbrokdepo,
                        v_taxholdbroksofr,
                        v_taxholdbrok,
                        v_taxholdmaterial,
                        v_taxholdbill,
                        v_taxholdonb,
                        one_rec.t_codekbk,
                        one_rec.t_rate);
        END LOOP;

      ELSIF(pSubKindOperation = RSI_NPTXC.DL_TXHOLD_OPTYPE_LUCRE) THEN 
        
        INSERT INTO dtaxcalctotal_tmp (t_taxholdbrokdepo,
                                       t_taxholdbroksofr,
                                       t_taxholdbrok,
                                       t_taxholdmaterial,
                                       t_taxholdbill,
                                       t_taxholdonb,
                                       t_codekbk,
                                       t_rate)
               SELECT 0,
                      0,
                      0,
                      SUM(t_taxcalc - t_taxhold),
                      0,
                      0,
                      t_codekbk,
                      t_rate
                 FROM dtaxecalcfunction_tmp 
                WHERE t_taxbasekind = RSI_NPTXC.NPTXTOTALBASE_TAXBASEKIND_BROK 
                  AND t_specialtag = 'МАТ'      
                GROUP BY t_codekbk, t_rate; 
      END IF;
    END;

    FUNCTION GetPriorOperNDFL(p_Rate IN INTEGER, p_TypeNOB IN INTEGER, p_KBK IN VARCHAR2, p_SpecialTag IN VARCHAR2, p_Sys IN INTEGER DEFAULT 1 ) 
      RETURN NUMBER DETERMINISTIC
    AS
      v_KBK   VARCHAR2(5);
      v_Prior INTEGER;
    BEGIN

      if(LENGTH(p_KBK) > 3) THEN --Если больше 3 символов, то полный КБК, а в таблице приоритетов только 3 символа
        BEGIN
          select t_code into v_KBK from DLLVALUES_DBT where T_LIST = 3522 and t_name = p_KBK;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN v_KBK := chr(1);
        END;
      else
        v_KBK := p_KBK;
      end if;

      if(LENGTH(v_KBK) <> 3) THEN --КБК не корректный 
        return 0;
      end if;

      BEGIN
        select t_priority into v_Prior 
          from dnptxpriorityndfl_dbt
         where t_rate = p_Rate
           and t_typenob = p_TypeNOB
           and t_kbk = v_KBK
           and (p_SpecialTag = chr(1) or t_specialtag = p_SpecialTag)
           and t_sys = p_Sys
           and rownum = 1;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN v_Prior := 0;
      END;

      RETURN v_Prior;
    END;

END RSI_NPTX;
/
