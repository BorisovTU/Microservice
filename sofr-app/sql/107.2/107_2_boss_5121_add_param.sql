declare
  vc_deal     varchar2(1000) := '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\QUIK\‚›ƒ“†€’œ DEALSLIB.INI'; 
  vc_deal_cap varchar2(1000) := '‚ë£àã¦ âì ä ©« DEALSLIB.INI ¤«ï QUIK';
  vc_codes     varchar2(1000) := '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\QUIK\‚›ƒ“†€’œ CODES.INI';
  vc_codes_cap varchar2(1000) := '‚ë£àã¦ âì ä ©« CODES.INI ¤«ï QUIK';
  vc_cros     varchar2(1000) := '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\QUIK\‚›ƒ“†€’œ CROSSRATE.INI';
  vc_cros_cap varchar2(1000) := '‚ë£àã¦ âì ä ©« CROSSRATE.INI ¤«ï QUIK';
  vc_crosp     varchar2(1000) := '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\QUIK\CROSSRATE_INI';
  vc_crosp_cap varchar2(1000) := 'ãáâì ª ä ©«ã CROSSRATE.INI';
  --
  vn number;
begin

  vn := it_rs_interface.add_parm_path(p_parm_path => vc_deal, p_type => 4, p_description => vc_deal_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 0);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_codes, p_type => 4, p_description => vc_codes_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
   vn := it_rs_interface.add_parm_path(p_parm_path => vc_cros, p_type => 4, p_description => vc_cros_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_crosp, p_type => 2, p_description => vc_crosp_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => '\\Quik-rezerv-nw\quik\CrossrateTW');
end;
/