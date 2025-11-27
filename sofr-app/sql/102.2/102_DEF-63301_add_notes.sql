DECLARE

   PROCEDURE InsertCode (p_partyid IN NUMBER, p_codeval IN VARCHAR2, p_startdate IN DATE)
   IS
   BEGIN
      
      DELETE FROM DOBJCODE_DBT WHERE T_OBJECTTYPE = cnst.OBJTYPE_PARTY AND T_OBJECTID = p_partyid AND T_CODEKIND=73;
      INSERT INTO DOBJCODE_DBT(  T_OBJECTTYPE, T_CODEKIND, T_OBJECTID, T_CODE, T_STATE, T_BANKDATE, T_SYSDATE, T_SYSTIME, T_USERID,
      T_BRANCH, T_NUMSESSION,T_UNIQUE, T_BANKCLOSEDATE )
     VALUES( cnst.OBJTYPE_PARTY, 73, p_partyid, p_codeval, 0,
       p_startdate,
       TRUNC(SYSDATE), TO_DATE('01.01.0001:' || TO_CHAR(SYSDATE, 'HH24:MI:SS'), 'DD.MM.YYYY:HH24:MI:SS'),
       RSBSESSIONDATA.OPER, 0, 0, CHR(0) ,TO_DATE('01.01.0001', 'DD.MM.YYYY'));
       
      DELETE FROM DOBJATCOR_DBT WHERE T_OBJECTTYPE = cnst.OBJTYPE_PARTY AND T_GROUPID = 12 AND T_OBJECT = LPAD( p_partyid, 10, '0' );

   END;
BEGIN
 InsertCode(339282, '58', to_date('18.12.2023','DD.MM.YYYY'));
 InsertCode(341622, '40', to_date('23.01.2024','DD.MM.YYYY'));
 InsertCode(341879, '46', to_date('26.01.2024','DD.MM.YYYY'));
 InsertCode(341881, '65', to_date('25.01.2024','DD.MM.YYYY'));
 InsertCode(341650, '46', to_date('22.01.2024','DD.MM.YYYY'));
 InsertCode(342586, '75', to_date('28.01.2024','DD.MM.YYYY'));
 InsertCode(343715, '50', to_date('27.02.2024','DD.MM.YYYY'));
 InsertCode(343718, '81', to_date('28.02.2024','DD.MM.YYYY'));
 InsertCode(343296, '28', to_date('19.02.2024','DD.MM.YYYY'));
 InsertCode(343297, '46', to_date('09.02.2024','DD.MM.YYYY'));
 InsertCode(343102, '65', to_date('06.02.2024','DD.MM.YYYY'));
 InsertCode(343103, '25', to_date('05.02.2024','DD.MM.YYYY'));
 InsertCode(343104, '34', to_date('07.02.2024','DD.MM.YYYY'));
 InsertCode(343105, '80', to_date('12.02.2024','DD.MM.YYYY'));
 InsertCode(342610, '46', to_date('31.01.2024','DD.MM.YYYY'));
 InsertCode(342612, '25', to_date('25.01.2024','DD.MM.YYYY'));
 InsertCode(343978, '46', to_date('29.02.2024','DD.MM.YYYY'));
 InsertCode(343979, '40', to_date('29.02.2024','DD.MM.YYYY'));
 InsertCode(332058, '55', to_date('02.10.2023','DD.MM.YYYY'));
 InsertCode(249936, '55', to_date('02.08.2022','DD.MM.YYYY'));
END;
/
