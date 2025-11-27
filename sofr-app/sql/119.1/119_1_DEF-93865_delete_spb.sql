  /************************************************\
    Скрипт скрипт отвязки дублирующих связей счетов ВУ
    По счетам СПБ по сделкам 07-05-2025
  \************************************************/

-- DEF-93865
declare
  x_I number := 0;
  x_Clob clob;
  x_Spb number := -1;
  x_Proc364 number := 0;
  x_Proc368 number := 0;
  x_LastLot number := 0;
  x_Moex364 number := -1;
  x_Spb364 number := -1;
  x_Moex368 number := -1;
  x_Spb368 number := -1;
  x_Acc364 varchar2(32);
  x_Acc368 varchar2(32);
  x_Cnt364 number := 0;
  x_Cnt368 number := 0;
  x_Pfi number := 0;
    function getLastLot(p_PARTY IN number, p_CONTRACT IN number, p_FIID IN number) 
       return number
    is
      x_LastLot number := 0;
    begin
      select nvl(r.t_amount, 0) As t_LastLot into x_LastLot
        from DPMWRTCL_DBT r 
        where r.T_PARTY = p_PARTY and r.T_CONTRACT = p_CONTRACT and r.T_FIID = p_FIID 
          and r.t_enddate = to_date('31.12.9999', 'dd-mm-yyyy') and rownum = 1
      ;
      return x_LastLot;
    EXCEPTION
      WHEN OTHERS THEN 
        return 0;
    end;
    function getAciveAcc(p_Cat IN number, p_PARTY IN number, p_CONTRACT IN number, p_FIID IN number) 
       return number
    is
      x_Acc number := -1;
    begin
      select r.t_id into x_Acc
        from dmcaccdoc_dbt r   
       where r.t_Chapter = 22 and r.t_iscommon = 'X' and r.t_catid = p_Cat
         and r.t_owner = p_PARTY and r.T_DISABLINGDATE = to_date('1-1-0001', 'dd-mm-yyyy')
         and r.t_clientcontrid = p_CONTRACT and r.t_currency = p_FIID
      ;
      return x_Acc;
    EXCEPTION
      WHEN OTHERS THEN 
        return -1;
    end;
begin
  for i in (
     SELECT mp.t_dlcontrid, t.t_clientid, t.t_clientcontrid AS moex_contrid, t.t_pfi, spb.t_sfcontrid AS spb_contrid
     FROM ddl_tick_dbt t
     join DDLCONTRMP_DBT mp ON (mp.t_sfcontrid = t.t_clientcontrid)
     join DDLCONTRMP_DBT spb ON (spb.t_dlcontrid = mp.t_dlcontrid)
     WHERE t.t_dealdate = TO_DATE ('07052025', 'ddmmyyyy')
     AND t.t_bofficekind = 127 AND t.t_oper = 1
     AND mp.t_marketid = 2 and mp.t_mpclosedate = to_date('01010001', 'ddmmyyyy')
     AND spb.t_marketid = 151337 and spb.t_mpclosedate = to_date('01010001', 'ddmmyyyy')
     GROUP BY mp.t_dlcontrid, t.t_clientid, t.t_clientcontrid, t.t_pfi, spb.t_sfcontrid
     ORDER BY mp.t_dlcontrid, t.t_clientid, spb.t_sfcontrid, t.t_pfi
  ) loop
    x_I := x_I + 1;
    IF(x_Spb <> i.spb_contrid) THEN
      x_Spb := i.spb_contrid;
      x_Pfi := 0;
    END IF;
    IF(x_Pfi <> i.t_pfi) THEN
      x_Pfi := i.t_pfi;
      x_Proc364 := 0; -- помечаем, как необработанный
      x_Proc368 := 0; -- помечаем, как необработанный
      x_LastLot := getLastLot(i.t_clientid, i.spb_contrid, i.t_pfi);
      x_Spb364 := getAciveAcc(364, i.t_clientid, i.spb_contrid, i.t_pfi);
      x_Spb368 := getAciveAcc(368, i.t_clientid, i.spb_contrid, i.t_pfi);
    END IF;
    -- Для необработанных договоров будем проверять условия,
    -- И если условия определят, что нужно будет произвести обработку,
    -- Тогда произведем обновление и изменим флаг
    IF((x_LastLot = 0) AND (x_Spb364 > 0)) THEN
      IF(x_Proc364 = 0) THEN
        x_Moex364 := getAciveAcc(364, i.t_clientid, i.moex_contrid, i.t_pfi);
        IF(x_Moex364 > 0) THEN
          x_Proc364 := 1;
          UPDATE DMCACCDOC_DBT mc SET mc.T_DISABLINGDATE=to_date('07-05-2025', 'dd-mm-yyyy')
           WHERE mc.t_id = x_Spb364 RETURNING mc.t_account INTO x_Acc364;
          it_log.log_handle( 'DEF-93865'
             , 'x_Moex364: '||x_Moex364
             ||', x_Spb364: '||x_Spb364 
             ||', x_Acc364: '||x_Acc364
          ) ;
          x_Cnt364 := x_Cnt364 + 1;
        END IF;
      END IF;
      IF(x_Proc368 = 0) THEN
        x_Moex368 := getAciveAcc(368, i.t_clientid, i.moex_contrid, i.t_pfi);
        IF(x_Moex368 > 0) THEN
          x_Proc368 := 1;
          UPDATE DMCACCDOC_DBT mc SET mc.T_DISABLINGDATE=to_date('07-05-2025', 'dd-mm-yyyy')
           WHERE mc.t_id = x_Spb368 RETURNING mc.t_account INTO x_Acc368;
           it_log.log_handle( 'DEF-93865'
             , 'x_Moex368: '||x_Moex368
             ||', x_Spb368: '||x_Spb368 
             ||', x_Acc368: '||x_Acc368
          ) ;
          x_Cnt368 := x_Cnt368 + 1;
        END IF;
      END IF;
    END IF;
  end loop;
  it_log.log_handle( 'DEF-93865'
     , 'Обновлено счетов '
     ||', по категории 364: '||x_Cnt364
     ||', по категории 368: '||x_Cnt368
  ) ;
end;
/

