declare
  l_object      it_rs_interface.tt_object;
begin
  delete from DMENUITEM_DBT where T_ICASEITEM = 16841;

  l_object(10010) := '[10] à¨ª« ¤­®©  ¤¬¨­¨áâà â®à';
  l_object(10036) := '[36] â¢¥âáâ¢¥­­ë© á®âàã¤­¨ª „”‹';

  it_rs_interface.add_menu_item_oper (
        p_cidentprogram   => 'S'
       ,p_menu_path       => ' «®£®¢ë© ãç¥â\„”‹'
       ,p_menu_item       => ' áç¥â ®¡à é ¥¬®áâ¨ æ/¡ ¤«ï „”‹'
       ,p_menu_nameprompt => ' áç¥â ®¡à é ¥¬®áâ¨ æ/¡ ¤«ï „”‹'
       ,p_usermodule_name => ' áç¥â ®¡à é ¥¬®áâ¨ æ/¡ ¤«ï „”‹'
       ,p_usermodule_file => 'nptx_recalc_circ.mac'
       ,pt_objectid       => l_object
       ,p_inumberline     => 200
    );
end;