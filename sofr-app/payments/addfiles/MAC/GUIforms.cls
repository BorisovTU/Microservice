import general, "mtserial.cls","empty.cls", "mtforms.cls", "GuiCntrs.cls", "guiserial.cls";
/* типы добавляемых в форму элементов */
var C_EDIT = 1, C_BUTTON = C_EDIT + 1, C_EDIT2SEC = C_EDIT + 2,
    C_EDIT3SEC = C_EDIT + 3, C_SCROLLBOX = C_EDIT + 4;

var FormsObjects = TArray (0), MT103SerializersGUI = TArray();

Class TMTErrorStreamVclMsgBox()
   Macro ShowMsg(_Msg)
      VclMsgBox(_Msg);
   End;
End;

Class TMTErrorStreamSilent()
   Macro ShowMsg(_Msg)
   End;
End;

MACRO GetCurrentFormObject():variant
  if (FormsObjects.Size>0)
    return FormsObjects.Value(FormsObjects.Size-1);
  else
    return null;
  end;
END;

/* является ли символ цифрой */
MACRO IsNum ( sym )
  return ((codefor(sym) > 47) AND (codefor(sym) < 58));
END;

MACRO SetActiveFormObject (obj)
 FormsObjects.Value(FormsObjects.Size) = obj;
END;

MACRO RestoreActiveFormObject()
var TempForms=TArray(FormsObjects);
var i=0;
    FormsObjects.Size=0;
    while(i<TempForms.Size-1)
      FormsObjects.Value(i)=TempForms.Value(i);
      i=i+1;
    end;
END;

MACRO DefFormProc (type, sender)
  var CurrForm=GetCurrentFormObject();
  var RetValue;
    RetValue=CurrForm.DefaultHandler(type, sender);
    return RetValue;
END;

CLASS TGUIMenu (strarray, capt, hght, wdth)
  var strings, caption, height, width;

  if (ValType (strarray) != V_UNDEF )
   this.strings  = TArray (strarray);
  else
   this.strings  = TArray ();
  end;
  this.caption = capt;
  this.height  = hght;
  this.width   = wdth;

  MACRO Copy (src)
     this.strings  = Tarray (src);
  END;

  MACRO Show ()
     Array tmp;
     var i, ret;

     i = 0;
     while (i < this.strings.size)
      tmp(i) = this.strings.Value(i);
      i = i + 1;
     end;
     ret = VclMenu (tmp, this.caption, this.height, this.width);
     return ret;
  END;

  MACRO Sort ()
   var i = 0, j, tmp;

   while ( i < this.strings.size)
      j = i;
      while (j  < this.strings.size)
       if (this.strings.Value(i) > this.strings.Value(j))
         tmp = this.strings.Value(i);
         this.strings.Value(i) = this.strings.Value(j);
         this.strings.Value(j) = tmp;
       end;
       j = j + 1;
      end;
      i = i + 1;
   end;

  END;


  MACRO AddString  (str)
     this.strings.Value(this.strings.size) = str;
  END;

  MACRO RemString (str)
     var newStrings = TArray (this.strings), i = 0;

     Strings.Size  = 0;
     if (ValType (str) == V_STRING)
      str = trim (str);
      while  (i < newstrings.size)
       if (newstrings.Value(i) != str)
        this.Strings.Value (this.Strings.size) = newstrings.Value(i);
       end;
       i = i + 1;
      end;
     elif (ValType (str) == V_INTEGER)
      while  (i < newstrings.size)
       if (i != str)
        this.Strings.Value (this.Strings.size) = newstrings.Value(i);
       end;
       i = i + 1;
      end;
     end;
  END;

END;

CLASS TScrollBox ()
var Controls = TArray(), Handle = 0, Top, Left, Width, Height, Parent, Name, FormHandle,
    LabelHandle = 0, Caption = "";

 MACRO ShowCaption ()
   var coeff = 6.5;
   if (this.Handle AND strlen (this.Caption))
     if (not this.LabelHandle)
       this.LabelHandle = ScrAddLabel (this.Parent, this.Top, this.Left - int(coeff *strlen(this.Caption))-
                                     LabelGap, this.Caption);
     else
      ScrShow (this.LabelHandle);
     end;
   end;
 END;

 MACRO Create (Name, Parent, Top, Left, Height, Width, Form, Caption)
   this.Top        = Top;
   this.Left       = Left;
   this.Width      = Width;
   this.Height     = Height;
   this.Parent     = Parent;
   this.Name       = trim(Name);
   this.FormHandle = Form;
   if (ValType (Caption) == V_STRING)
    this.Caption    = trim(Caption);
   end;
   this.Handle  = ScrAddScroll(this.Parent, this.Top, this.Left, this.Height,
                               this.Width);
   this.ShowCaption();
 END;


 MACRO MyHandle (hndl)
  var i = 0, ret = false;
  if (hndl == this.Handle)
   i = Controls.Size;
   ret = true;
  end;
  while (i < Controls.Size)
    if (Controls.Value(i).MyHandle(hndl))
     ret = true;
     i = Controls.Size;
    end;
    i = i + 1;
  end;
  return ret;
 END;


 MACRO Show ()
     ScrShow (this.Handle);
     if (this.LabelHandle)
      ScrShow(this.LabelHandle);
     end;
 END;

 MACRO Hide ()
     ScrHide (this.Handle);
     if (this.LabelHandle)
      ScrHide(this.LabelHandle);
     end;
 END;

 MACRO GetTop ()
   return this.Top;
 END;

 MACRO GetLeft ()
  return this.Left;
 END;

 MACRO GetWidth ()
  if (this.Width)
   return this.Width;
  else
   return  30;
  end;
 END;

 MACRO GetHeight ()
  if (this.Height)
   return this.Height;
  else
   return 20;
  end;
 END;

 MACRO Move (NewX, NewY)
   var i = 0, TmpArray, OldHandle, tmpCntrl;

   if (this.Handle AND ((NewX != this.Left) OR (NewY != this.Top)))
      this.Top  = NewY;
      this.Left = NewX;
      OldHandle = this.Handle;
      this.Hide();

      this.Handle = ScrAddScroll(this.Parent, this.Top, this.Left, this.Height, this.Width);
      if (this.LabelHandle)
       ScrDelete (this.LabelHandle);
       this.LabelHandle = 0;
       ShowCaption ();
      end;
      TmpArray = TArray (this.Controls);
      this.Controls.size = 0;
      while (i < TmpArray.Size)
       if (strupr(GenClassName (TmpArray.Value(i))) == "TEDIT")
        tmpCntrl = this.AddControl(C_EDIT, TmpArray.Value(i).Name,
                                   TmpArray.Value(i).GetTop(),
                                   TmpArray.Value(i).GetLeft(),
                                   TmpArray.Value(i).GetHeight(),
                                   TmpArray.Value(i).GetWidth(),
                                   TmpArray.Value(i).GetText(),
                                   TmpArray.Value(i).Mode,
                                   TmpArray.Value(i).BtnType,
                                   TmpArray.Value(i).MaxLen,
                                   TmpArray.Value(i).NameHidden);
        if (TmpArray.Value(i).ReadOnly)
         tmpCntrl.ReadOnlySet();
        end;
       end;
       i = i + 1;
      end;
      ScrDelete (OldHandle);
   end;
   return (this.Handle == 0);
 END;

 MACRO Remove ()
    var i = 0;

    if (this.Handle)
      ScrDelete (this.Handle);
    end;
    if (this.LabelHandle)
       ScrDelete (this.LabelHandle);
    end;
    while ( i< this.controls.size)
      this.controls.Value(i).Remove();
      i = i + 1;
    end;
    this.controls.size = 0;
 END;

 MACRO IsMyControl (obj)
  var i = 0, ret = false;
  while (i < Controls.Size)
    if (Controls.Value(i) == obj)
     ret = true;
     i = Controls.Size;
    end;
    i = i + 1;
  end;
  return ret;
 END;

 MACRO GetControlIndex (obj)
  var i = 0, ret = 0;

  i = this.Controls.Size - 1;
  while (i >= 0)
    if (this.Controls.Value(i).Handle == obj.Handle)
      ret = i;
      i = -1;
    end;
    i = i - 1;
  end;
  return ret;
 END;

 MACRO ControlByName(name )
  var i = -1, ret = 0;

  name = trim(name);
  if (name == this.Name)
   ret = this;
  else
   i = this.Controls.Size - 1;
  end;

  while (i >= 0)
    if (strupr(GenClassName (this.Controls.Value(i))) == "TSCROLLBOX")
     ret = this.Controls.Value(i).ControlByName(name);
     if (ValType(ret) == V_GENOBJ)
       i = -1;
     end;
    else
     if (this.Controls.Value(i).Name == Name)
       ret = this.Controls.Value(i);
       i = -1;
     end;
    end;
    i = i - 1;
  end;
  return ret;
 END;

 MACRO ControlByHandle (hndl)
  var i = 0, ret = 0;
  if (hndl == this.Handle)
   ret = this;
   i = Controls.Size;
  end;
  while (i < Controls.Size)
    if (Controls.Value(i).MyHandle(hndl))
     if ((strupr(GenClassName (Controls.Value(i))) == "TSCROLLBOX"))
      ret = Controls.Value(i).ControlByHandle(Hndl);
     else
      ret = Controls.Value(i);
     end;
      i = Controls.Size;
    end;
    i = i + 1;
  end;
  return ret;
 END;

 MACRO AddControl (Type)
   var obj, params, i , val, flag = true;

     i = 2;
     params = TArray();
     while (flag)
       flag = GetParm (i, val);
       if (flag)
        params.Value(params.Size) = val;
       end;
       i = i + 1;
     end;

     if (i == 2 )
      return ;
     end;
     if  (Type == C_EDIT)
       obj  = TEdit();
       obj.Create (params.Value(0), this.Handle, params.Value(1),
                   params.Value(2), params.Value(3), params.Value(4),
                   params.Value(5), params.Value(6), this.FormHandle, params.Value(7),
                   params.Value(8));
       if ((ValType (params.Value(9)) == V_BOOL) AND (params.Value(9)))
         obj.HideName();
       end;
     elif(Type == C_BUTTON)
       obj = TButton();
       obj.Create (params.Value(0), this.Handle, params.Value(1),
                   params.Value(2), params.Value(3), params.Value(4),
                   params.Value(5), params.Value(6), this.FormHandle);

     elif(Type == C_EDIT2SEC)
       obj = TEdit2Sec();
       obj.Create (params.Value(0), this.Handle,
                   params.Value(1), params.Value(2), params.Value(3),
                   params.Value(4), params.Value(5), this.FormHandle,
                   params.Value(6), params.Value(7));
     elif(Type == C_EDIT3SEC)
       obj = TEdit3Sec();
       obj.Create (params.Value(0), this.Handle,
                   params.Value(1), params.Value(2), params.Value(3),
                   params.Value(4), params.Value(5), params.Value(6),
                   params.Value(7), this.FormHandle, params.Value(8),
                   params.Value(9));
     elif(Type == C_SCROLLBOX)
       obj = TScrollBox();
       obj.Create (params.Value(0), this.Handle, params.Value(1),
                   params.Value(2), params.Value(3), params.Value(4),
                   this.FormHandle, params.Value(5));

     end;
     this.Controls.Value(Controls.Size) = obj;
     return obj;
 END;

 MACRO RemoveControl (val)
   var i = 0, tmpArray;

   if (ValType (val) == V_STRING)
     val = this.ControlByName (trim (val));
   end;
   if (valtype (val) == V_INTEGER)
     return;
   end;

   tmpArray = TArray (this.Controls);
   this.Controls.size = 0;

   while (i < tmpArray.size )
    if ((strupr(GenClassName(tmpArray.Value(i))) == "TSCROLLBOX"))
     if (tmpArray.Value(i).Handle != val.Handle)
      tmpArray.Value(i).RemoveControl(val);
      this.Controls.Value (this.Controls.size) = tmpArray.Value(i);
     else
      tmpArray.Value(i).Remove();
     end;
    elif (tmpArray.Value(i).Handle!= val.Handle)
     this.Controls.Value (this.Controls.size) = tmpArray.Value(i);
    else
     tmpArray.Value(i).Remove();
    end;
    i = i + 1;
   end;
 END;

END;


CLASS TGUIForm ()
var Controls = TArray(), Handle = 0, Top, Left, Width, Height, Caption, Ydist = 5;

 MACRO Create (Top, Left, Height, Width, Caption)
   this.Top     = Top;
   this.Left    = Left;
   this.Width   = Width;
   this.Height  = Height;
   this.Caption = Caption;
   this.Handle  = ScrCreate(this.Top, this.Left, this.Height,
                       this.Width, this.Caption, "DefFormProc");

 END;

 MACRO Show ()
     ScrShow (this.Handle);
 END;

 macro ChSubStr(Str,SStr,CStr)
   var TmpStr="",
   Pos=0;

   Pos = Index(Str,SStr);
   while(Pos)
         TmpStr = TmpStr+Trim(SubStr(Str, 1, Pos - 1))+CStr;
         Str = SubStr(Str, Pos + StrLen(SStr));
         Pos = Index(Str,SStr);
   end;
   return Trim(TmpStr+Str);
 end;

 MACRO ShowModal ()
   var result = 0;
   SetActiveFormObject (this);
   result = ScrShowModal (this.Handle);
   RestoreActiveFormObject ();
   return result;
 END;

 MACRO Hide ()
     ScrHide (this.Handle)
 END;

 MACRO IsMyControl (obj)
  var i = 0, ret = false;

  while (i < Controls.Size)
    if (this.Controls.Value(i) == obj)
     ret = true;
     i = this.Controls.Size;
    end;
    i = i + 1;
  end;
  return ret;
 END;

 MACRO GetControlIndex (obj)
  var i = 0, ret = 0;

  i = this.Controls.Size - 1;
  while (i >= 0)
    if (this.Controls.Value(i).Handle == obj.Handle)
      ret = i;
      i = -1;
    end;
    i = i - 1;
  end;
  return ret;
 END;

 MACRO ControlByName(name )
  var i = 0, ret = 0;

  name = trim(name);
  i = this.Controls.Size - 1;

  while (i >= 0)
    if (strupr(GenClassName (this.Controls.Value(i))) == "TSCROLLBOX")
     ret = this.Controls.Value(i).ControlByName(name);
     if (ValType(ret) == V_GENOBJ)
       i = -1;
     end;
    else
     if (this.Controls.Value(i).Name == Name)
       ret = this.Controls.Value(i);
       i = -1;
     end;
    end;
    i = i - 1;
  end;
  return ret;
 END;

 MACRO ControlByHandle (hndl)
  var i = 0, ret = 0;
  if (hndl == this.Handle)
   ret = this;
   i = Controls.Size;
  end;
  while (i < this.Controls.Size)
    if (this.Controls.Value(i).MyHandle(hndl))
     if ((strupr(GenClassName (this.Controls.Value(i))) == "TSCROLLBOX"))
      ret = this.Controls.Value(i).ControlByHandle(Hndl);
     else
      ret = this.Controls.Value(i);
     end;
     i = this.Controls.Size;
    end;
    i = i + 1;
  end;
  return ret;
 END;

 MACRO AddControl (Type)
   var obj, params, i, val , flag = true;

     i = 2;
     params = TArray();

     while (flag)
       flag = GetParm (i, val);
       params.Value(params.Size) = val;
       i = i + 1;
     end;
     if (i ==  2 )
      return ;
     end;
     if  (Type == C_EDIT)
       obj  = TEdit();
       obj.Create (params.Value(0), this.Handle, params.Value(1),
                   params.Value(2), params.Value(3), params.Value(4),
                   params.Value(5), params.Value(6), this.Handle,
                   params.Value(7), params.Value(8));
       if ((ValType (params.Value(9)) == V_BOOL) AND (params.Value(9)))
         obj.HideName();
       end;
     elif(Type == C_BUTTON)
       obj = TButton();
       obj.Create (params.Value(0), this.Handle, params.Value(1),
                   params.Value(2), params.Value(3), params.Value(4),
                   params.Value(5), params.Value(6));

     elif(Type == C_EDIT2SEC)
       obj = TEdit2Sec();
       obj.Create (params.Value(0), this.Handle,
                   params.Value(1), params.Value(2), params.Value(3),
                   params.Value(4), params.Value(5), this.Handle,
                   params.Value(6), params.Value(7));
     elif(Type == C_EDIT3SEC)
       obj = TEdit3Sec();
       obj.Create (params.Value(0), this.Handle,
                   params.Value(1), params.Value(2), params.Value(3),
                   params.Value(4), params.Value(5), params.Value(6),
                   params.Value(7), this.Handle, params.Value(8),
                   params.Value(9));
     elif(Type == C_SCROLLBOX)
       obj = TScrollBox();
       obj.Create (params.Value(0), this.Handle, params.Value(1),
                   params.Value(2), params.Value(3), params.Value(4),
                   this.Handle, params.Value(5));

     end;
     this.Controls.Value(Controls.Size) = obj;
     return obj;
 END;

 MACRO RemoveControl (val)
   var i = 0, tmpArray, tmp;

   if (ValType (val) == V_STRING)
     val = this.ControlByName (trim(val));
   end;
   if (valtype (val) == V_INTEGER)
     return;
   end;
   tmpArray = TArray (this.Controls);
   this.Controls.size = 0;
   while (i < tmpArray.size )
    if ((strupr(GenClassName(tmpArray.Value(i))) == "TSCROLLBOX"))
     if (tmpArray.Value(i).Handle != val.Handle)
      tmpArray.Value(i).RemoveControl(val);
      this.Controls.Value (this.Controls.size) = tmpArray.Value(i);
     else
      tmpArray.Value(i).Remove();
     end;
    elif (tmpArray.Value(i).Handle != val.Handle)
     this.Controls.Value (this.Controls.size) = tmpArray.Value(i);
    else
     tmpArray.Value(i).Remove();
    end;
    i = i + 1;
   end;
 END;

 MACRO FldNumFromName (str)
   var i = 1 , t= "", sym = "";

   while (i < Strlen (str) + 1)
       sym  = substr (str, i, 1);
       if ((codefor(sym) > 47 ) AND (codefor(sym) <  58 ))
        t = t + sym;
       end;
       i = i + 1;
   end;
   return t;
 END;

 MACRO MoveFieldsDown (parent, obj, dist)
   var i, y ;

   i = 0;
   y = obj.GetTop ();
   while (i < parent.Controls.size )
     if (y < parent.Controls.Value(i).GetTop())
      parent.Controls.Value(i).Hide();
      parent.Controls.Value(i).Move (parent.Controls.Value(i).GetLeft(),
                                           parent.Controls.Value(i).GetTop() +
                                           dist + this.YDist);
     end;
     i = i +1;
   end;
 END;

 MACRO MoveFieldsUP (parent, obj, dist)
  var i, y;

  i = 0;
  y = obj.GetTop ();
  while (i < parent.Controls.size )
     if (y < parent.Controls.Value(i).GetTop())
      parent.Controls.Value(i).Hide();
      parent.Controls.Value(i).Move (parent.Controls.Value(i).GetLeft(),
                                           parent.Controls.Value(i).GetTop() -
                                           dist - this.YDist);
     end;
     i = i +1;
  end;
 END;

 MACRO OnButtonClick (sender)
   return 0;
 END;

 MACRO OnFocusRemove (sender)
  return 0;
 END;

 MACRO OnLkpButtonClick (sender)
  return 0;
 END;

 MACRO DefaultHandler (type, sender)
    if  (type == 1)
      return this.OnButtonClick(sender);
    elif(type == 2)
      return this.OnFocusRemove(sender);
    elif(type == 3)
      return this.OnLkpButtonClick(sender);
    end;
 END;


END;

CLASS (TGUIForm) TLkpFormF32 (_Owner)
 var Owner = _Owner;

 MACRO DateStr2Swift(dateStr)
   dateStr=ChSubStr(dateStr,"_","0");
   dateStr=ChSubStr(dateStr," ","0");
   return substr(dateStr,9,2)+substr(dateStr,4,2)+substr(dateStr,1,2);
 END;

 MACRO Swift2Date(dateStr)
   var yy, mm, dd;
   yy = Int(SubStr(dateStr, 1, 2));
   mm = Int(SubStr(dateStr, 3, 2));
   dd = Int(SubStr(DateStr, 5, 2));
   If (yy > 80)
      yy = yy + 1900;
   Else
      yy = yy + 2000;
   End;
   return date(dd,mm,yy);
 END;

 MACRO OnButtonClick (sender)
  var curControl = ControlByHandle(sender), Amnt, Code, Data;
  if (curControl.Name == "BtnSave")
   Code = this.ControlByname("Валюта").GetText();
   Amnt = this.ControlByname("Сумма").GetText();
   Data = this.ControlByname("Дата").GetText();
   if(Code =="")
     VclMsgBox("Не заполнено поле \"ВАЛЮТА\"");
     return 0;
   elif (Data == "")
     VclMsgBox("Не заполнено поле \"Дата валютирования \"");
     return 0;
   elif( double(amnt)==0)
     VclMsgBox("Не заполнено поле \"СУММА\"");
     return 0;
   end;
   Amnt = ChSubStr(Amnt,".",",");
   if(NOT Index(Amnt,","))
     Amnt=Amnt + ",";
   end;
   if (strlen (Substr (Amnt, Index(Amnt, ",") + 1)) > 2)
    VclMsgBox("Много знаков после запятой ");
    return 0;
   end;
   Owner.SetText (Owner.GetText(0),DateStr2Swift(Data) + Code + Amnt);
   return 1;
  else
   return 2;
  end;
 END;

 MACRO OnLkpButtonClick (sender)
  var val, tmpval, curControl;
  curControl = ControlByHandle(sender);
  val = curControl.GetText();
  if   (curControl.Name == "Валюта")
//     if( DoLkp(10,1,"", "StrCode", val) )
     if(DoLkp(GetIdByCode("FORM","ID_FORM",10),"","STRCODE", val))
         curControl.SetText(val);
     End;
  end;
 END;

 MACRO Create ()
   var tmp, text = "", data;
   this.Top    = 0;
   this.Left   = 0;
   this.Height = 180;
   this.Width  = 310;
   this.Handle  = ScrCreate(this.Top, this.Left,this.Height, this.Width, "Поле " + this.Owner.Name, "DefFormProc");
   text = this.Owner.GetText(1);
   data = Substr (text, 1, 6);
   if (data == "")
    data = {curdate};
   else
    data = Swift2Date (data);
   end;
   data= ChSubStr(string(data), "-", ".");
   tmp = this.AddControl(C_BUTTON,"BtnSave", this.Height - 65, 40, 25, 80, "OK", 0);
   tmp = this.AddControl(C_BUTTON,"BtnQuit", tmp.GetTop(), tmp.GetLeft() + tmp.GetWidth() + 65, 25, 80,"Cancel", 0);

   tmp = this.AddControl(C_EDIT,"Дата", 10, 50, 0, 85, data, ED_DATE, 0, 6);
   tmp = this.AddControl(C_EDIT,"Валюта", tmp.GetTop() + tmp.GetHeight() + YDist,
                    tmp.GetLeft(), 0, 55, substr (text, 7, 3), ED_STRING, 3, 3);
   tmp = this.AddControl(C_EDIT,"Сумма", tmp.GetTop() + tmp.GetHeight() + YDist,
                    tmp.GetLeft(), 0, 0 , substr (text, 10) , ED_MONEY, 0,15);
 END;
 InitTGUIform();
END;

CLASS (TGUIForm) TLkpFormFIIDSum (_Owner)
 var Owner = _Owner;

 MACRO OnButtonClick (sender)
  var curControl = ControlByHandle(sender), Amnt, Code;
  if (curControl.Name == "BtnSave")
    Code = this.ControlByname("Валюта").GetText();
    Amnt = this.ControlByname("Сумма").GetText();
    if(Code =="")
      VclMsgBox("Не заполнено поле ВАЛЮТА");
      return 0;
    elif( double(amnt)==0 )
      VclMsgBox("Не заполнено поле СУММА");
      return 0;
    end;
    Amnt = ChSubStr(Amnt,".",",");
    if(NOT Index(Amnt,","))
      Amnt=Amnt + ",";
    end;
    if (strlen (Substr (Amnt, Index(Amnt, ",") + 1)) > 2)
     VclMsgBox("Много знаков после запятой ");
     return 0;
    end;
    Owner.SetText (Code + Amnt);
    return 1;
  else
   return 2;
  end;
 END;

 MACRO OnLkpButtonClick (sender)
  var val, tmpval, curControl;
  curControl = ControlByHandle(sender);
  val = curControl.GetText();
  if   (curControl.Name == "Валюта")
//     if( DoLkp(10,1,"", "StrCode", val) )
     if(DoLkp(GetIdByCode("FORM","ID_FORM",10),"","STRCODE", val))
         curControl.SetText(val);
     End;
  end;
 END;

 MACRO Create ()
   var tmp, text = "";
   this.Top    = 0;
   this.Left   = 0;
   this.Height = 160;
   this.Width  = 310;
   this.Handle  = ScrCreate(this.Top, this.Left,this.Height, this.Width, "Поле " + this.Owner.Name, "DefFormProc");
   text = this.Owner.GetText(1);
   tmp = this.AddControl(C_BUTTON,"BtnSave", this.Height - 65, 40, 25, 80, "OK", 0);
   tmp = this.AddControl(C_BUTTON,"BtnQuit", tmp.GetTop(), tmp.GetLeft() + tmp.GetWidth() + 65, 25, 80,"Cancel", 0);
   tmp = this.AddControl(C_EDIT,"Валюта", 10, 50, 0, 55, substr (text, 1, 3), ED_STRING, 3, 3);
   tmp = this.AddControl(C_EDIT,"Сумма", tmp.GetTop() + tmp.GetHeight() + YDist,
                         tmp.GetLeft(), 0, 0 , substr (text, 4) , ED_MONEY, 0,15);
 END;
 InitTGUIform();
END;

CLASS (TGUIForm) TLkpFormF5X (_Owner, _Source)
 var Owner = _Owner, Source = _Source;


 MACRO OnButtonClick (sender)
  var curControl = ControlByHandle(sender), BIC, PI, Loc, NA, fldNA, i, fldOption;

  if (curControl.Name == "BtnSave")
   BIC = this.ControlByName ("BIC").GetText ();
   PI  = this.ControlByName ("PI").GetText ();
   Loc = this.ControlByName ("Loc");
   if (strlen (PI) AND not index (PI, "/"))
    PI = "/" + PI;
   end;
   if (valType(Loc) != V_INTEGER)
     Loc = Loc.GetText ();
   else
     Loc = "";
   end;
   NA  = this.ControlByName("NA").GetText ();
   i = 1;
   while (i < 4)
    if (NA != "")
     NA = NA + RN + this.ControlByName("NA" + i).GetText ();
    else
     NA = NA + this.ControlByName("NA" + i).GetText ();
    end;
    i = i + 1;
   end;
   if   ((BIC!="") AND (Loc!=""))
    VclMsgBox  ("Поля Location и БИК - взаимоисключающие");
    return 0;
   elif ((BIC!="") AND (NA!=""))
    VclMsgBox  ("Поля BIC и Name/Address - взаимоисключающие");
    return 0;
   elif ((Loc!="") AND (NA!=""))
    VclMsgBox  ("Поля Location и Name/Address - взаимоисключающие");
    return 0;
   end;
   If (BIC != "")
    if (Index (Source.AvailableOptions, "A" ))
     Owner.SetText ("A", PI, BIC);
    else
     VclMsgBox  ("Форма неправильно заполнена");
    end;
   Elif (Loc != "")
      Owner.SetText ("B", PI, BIC);
      if (Index (Source.AvailableOptions, "B" ))
       Owner.SetText ("B", PI, BIC);
      else
       VclMsgBox  ("Форма неправильно заполнена");
      end;
   Elif (NA != "")
     if (Owner.Name == "F50")
       if ((index(NA, "/") == 2) and IsNum(NA))
        fldOPtion = "F";
       elif (Index (Source.AvailableOptions, "K" ))
        fldOPtion = "K";
       else
        fldOPtion = "";
       end;
     elif (Owner.Name == "F59")
       if ((index(NA, "/") == 2) and IsNum(NA))
        fldOPtion = "F";
       else
        fldOPtion = "";
       end;
     else
       fldOption = "D";
     end;
     fldNa = NA;
     /*i = 0;
     fldNa  = "";
     while (strlen(NA) > 0)
       if (strlen (NA) > 35)
        fldNa = fldNa + trim(Substr (NA, 1, 35)) + RN;
       else
        fldNa = fldNa + trim(Substr (NA, 1, 35));
       end;
       NA = trim(Substr (NA, 36));
     end;*/
     Owner.SetText (fldOption, PI, fldNA);
   elif ((BIC == "") AND (Loc == "") AND (NA == "") AND (PI != "") AND
         ((Owner.Name == "F56")OR(Owner.Name == "F57")))
      if (Index (Source.AvailableOptions, "C" ))
       Owner.SetText ("C", PI, BIC);
      else
       VclMsgBox  ("Форма неправильно заполнена");
      end;
      Owner.SetText ("C", PI, "");
   Else
      /*Owner.SetText ("", "", "");*/
      VclMsgBox  ("Форма неправильно заполнена");
      return 0;
   End;
   return 1;
  else
   return 2;
  end;
 END;

 MACRO OnLkpButtonClick (sender)
 /*!! Т.к., от справочника банков в Пейментсе отказались, этот функционал не актуален. Да и вообще, похоже, эта функция не нужна - эдиты полей БИК и ПИ без кнопок лукапа.*/
  var val, tmpval, curControl;
  curControl = ControlByHandle(sender);
  val = curControl.GetText();
  if   (curControl.Name == "BIC")
     if( DoLkp(1,1,"", "type",tmpval, "bic",val) )
         curControl.SetText(val);
     End;
  elif (curControl.Name == "PI")
     if( DoLkp(1001,1,"", "Code", tmpVal, "ShortName", val) )
        curControl.SetText(val);
     End;
  end;
 END;

 MACRO Create ()
   var tmp, str, i, Opt;
   Array NA;
   this.Top    = 0;
   this.Left   = 0;
   this.Height = 250;
   this.Width  = 310;
   this.Handle  = ScrCreate(this.Top, this.Left,this.Height, this.Width, "Поле " + this.Owner.Name, "DefFormProc");
   tmp = this.AddControl(C_BUTTON,"BtnSave", this.Height - 65, 40, 25, 80, "OK", 0);
   tmp = this.AddControl(C_BUTTON,"BtnQuit", tmp.GetTop(), tmp.GetLeft() + tmp.GetWidth() + 65, 25, 80,"Cancel", 0);
   Opt = Owner.GetText(0);
   if (Opt == "A")
    str = ChSubstr(this.Owner.GetText(2), RN, "");
   else
    str = "";
   end;

   tmp = this.AddControl(C_EDIT,"BIC", 10, 30, 0, 85, str , ED_STRING, 0, 11);

   tmp = this.AddControl(C_EDIT,"PI", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft(), 0, 250, this.Owner.GetText (1) , ED_STRING, 0, 37);
   if (((this.Owner.Name == "F53") OR (this.Owner.Name == "F54") OR
       (this.Owner.Name == "F55") OR (this.Owner.Name == "F57")) AND
       ((Opt == "B") OR (Opt == "")))
    tmp = this.AddControl(C_EDIT,"Loc", tmp.GetTop() + tmp.GetHeight() + YDist,
                    tmp.GetLeft(), 0, 250, ChSubstr(this.Owner.GetText(2), RN, ""), ED_STRING, 0, 35);
   end;
   asize (NA,0);
   if ((Opt == "D") OR (Opt == "K") OR (Opt == "" ))
    StrSplit (ChSubstr(this.Owner.GetText(2), RN, ""), NA, 35, 35, 4);
   else
    StrSplit ("", NA, 35, 35, 4);
   end;
   tmp = this.AddControl(C_EDIT,"NA", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 0, 250, NA(0), ED_STRING, 0, 35);
   i = 1;
   while (i < 4)
    tmp = this.AddControl(C_EDIT,"NA"+string(i),
                          tmp.GetTop() + tmp.GetHeight() + YDist,
                          tmp.GetLeft(), 0, 250, NA(i), ED_STRING, 0, 35);
    tmp.HideName();
    i = i + 1;
   end;
   if (Owner.Name == "F51")
       this.controlbyName ("NA").ReadOnlySet();
       this.controlbyName ("NA1").ReadOnlySet();
       this.controlbyName ("NA2").ReadOnlySet();
       this.controlbyName ("NA3").ReadOnlySet();
   end;
 END;
 InitTGUIform();
END;


CLASS (TGUIForm) TGUIForm103
  var AddMenu, RemMenu, DataSource;


 MACRO AddRepeatableField (Name, Val, ScrollHeight, ScrollWidth, Width, MaxSym, ControlBe4, BtnType, ReadOnly)
   var scroll, tmp;
    scroll = this.ControlByName ("Scroll" + Name);
    if (ValType (scroll) == V_INTEGER)
     scroll = this.ControlByName ("MainScroll");
     tmp = ControlBe4;
     this.MoveFieldsDown (scroll, tmp, ScrollHeight);
     tmp = scroll.AddControl (C_SCROLLBOX, "Scroll" + Name,
                              tmp.GetTop() + tmp.GetHeight() + this.YDist,
                              tmp.GetLeft(), ScrollHeight, ScrollWidth, Name);
     tmp = tmp.AddControl (C_EDIT, Name, 10, 10, 0, Width,
                           Val,ED_STRING, BtnType, MaxSym, true);
     this.RemMenu.AddString (Name);
    else
     tmp = scroll.Controls.Value(scroll.Controls.Size - 1);
     tmp = scroll.AddControl (C_EDIT, Name,
                        tmp.GetTop () + tmp.GetHeight() + this.YDist,
                        tmp.GetLeft(), tmp.GetHeight(), tmp.GetWidth(), Val,
                        ED_STRING, BtnType, MaxSym, true);

    end;
    if ((ValType(ReadOnly) == V_BOOL) AND ReadOnly)
     tmp.ReadOnlySet();
    end;
    return tmp;
 END;

 MACRO RemoveFromAddMenu (Name)
    this.RemMenu.AddString (Name);
    this.AddMenu.RemString (Name);
 END;

 MACRO AddFld13D (Name, Text)
  var tmp, scroll;
  scroll = this.ControlByName ("MainScroll");
  tmp = this.ControlByName ("F25");
  this.MoveFieldsDown (scroll, tmp, 19);
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    0, 130, Text, ED_STRING, 0, 140);

  RemoveFromAddMenu (Name);
 END;

 MACRO AddFld5X (Name, Opt, Pi, Na)
   var tmp, scroll, i, lastedt;

   scroll = this.ControlByName ("MainScroll");
   tmp = scroll.ControlByName ("F50");
   i = int(FldNumFromName (Name)) - 1;
   while (i > 50)
     lastedt = scroll.ControlByName ("F" + string(i));
     if (Valtype (lastedt) != V_INTEGER)
      tmp = lastedt;
      i = 50;
     end;
     i = i - 1;
   end;
   this.MoveFieldsDown (scroll, tmp, tmp.GetHeight());
   tmp = Scroll.AddControl(C_EDIT3SEC, Name,
                            tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft(), 50, 250, Opt, Pi, Na, 3, 0);
   tmp.ReadOnlySet();
   RemoveFromAddMenu (Name);
 END;

 MACRO AddFld26T (Name, Text)
   var tmp, scroll;

   scroll = this.ControlByName ("MainScroll");
   tmp = this.ControlByName ("ScrollF23");
   if (ValType(tmp) == V_INTEGER)
     tmp = this.ControlByName ("F23B");
   end;
   this.MoveFieldsDown (scroll, tmp, tmp.GetHeight());
   Scroll.AddControl(C_EDIT, Name,
                     tmp.GetTop() + tmp.GetHeight() + YDist,
                     tmp.GetLeft(), 0, 40, Text , ED_STRING, 0, 3);
   RemoveFromAddMenu (Name);
 END;

 MACRO AddFld33B (Name, Text)
  var tmp, scroll;

  scroll = this.ControlByName ("MainScroll");
  tmp = scroll.ControlByName ("F32");
  this.MoveFieldsDown (scroll, tmp, tmp.GetHeight());
  tmp = Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    tmp.GetLeft(), 0, 130, Text, ED_STRING, 3, 18);
  tmp.ReadOnlySet();
  RemoveFromAddMenu (Name);
 END;


 MACRO AddFld36 (Name, Text)
  var tmp, scroll;

  scroll = this.ControlByName ("MainScroll");

  tmp = scroll.ControlByName ("F33B");
  if (ValType(tmp) == V_INTEGER)
    tmp = this.ControlByName ("F32");
  end;
  this.MoveFieldsDown (scroll, tmp, tmp.GetHeight());
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    tmp.GetLeft(), 0, 130, Text, ED_STRING, 0, 12);
  RemoveFromAddMenu (Name);
 END;

 MACRO AddFld70 (Name, Text)
  var tmp, scroll;

  tmp = this.ControlByName ("F59");
  scroll = this.ControlByName ("MainScroll");
  this.MoveFieldsDown (scroll, tmp, 58);
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    58, 250, Text, ED_STRING, 0, 140);
  RemoveFromAddMenu (Name);
 END;

 MACRO AddFld71G (Name, Text)
  var tmp, scroll;

  tmp = this.ControlByName ("ScrollF71F");
  scroll = this.ControlByName ("MainScroll");
  if (ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("F71A");
  end;
  this.MoveFieldsDown (scroll, tmp, scroll.ControlByName ("F71A").GetHeight());
  tmp = Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    0, 130, Text, ED_STRING, 3, 18);
  tmp.ReadOnlySet();
  RemoveFromAddMenu (Name);
 END;

 MACRO AddFld72 (Name, Text)
  var tmp, scroll;

  tmp = this.ControlByName ("F71G");
  scroll = this.ControlByName ("MainScroll");
  if (ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("ScrollF71F");
  end;
  if (ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("F71A");
  end;
  if (ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("F57");
  end;
  this.MoveFieldsDown (scroll, tmp, 85);
  Scroll.AddControl(C_EDIT, Name,
                            tmp.GetTop() + tmp.GetHeight() + YDist,
                            this.ControlByName ("F20").GetLeft(),
                            85, 250, Text, ED_STRING, 0, 210);
  RemoveFromAddMenu (Name);
 END;

 MACRO AddFld77B (Name, Text)
  var tmp, scroll;

  tmp = this.ControlByName ("F72");
  scroll = this.ControlByName ("MainScroll");
  if(ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("F71G");
  end;
  if  (ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("ScrollF71F");
  end;
  if  (ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("F71A");
  end;
  this.MoveFieldsDown (scroll, tmp, 50);
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    50, 250, Text , ED_STRING, 0, 105);
  RemoveFromAddMenu (Name);
 END;

 MACRO AddFld77T (Name, Text)
  var tmp, scroll;

  tmp = this.ControlByName ("F77B");
  scroll = this.ControlByName ("MainScroll");
  if(ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("F72");
  end;
  if(ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("F71G");
  end;
  if  (ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("ScrollF71F");
  end;
  if  (ValType (tmp) == V_INTEGER)
    tmp = this.ControlByName ("F71A");
  end;
  this.MoveFieldsDown (scroll, tmp, 160);
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    160, 400, "" , ED_STRING, 0, 9000);
  RemoveFromAddMenu (Name);
 END;


 MACRO OnButtonClick (sender)
   var tmp, idx, retval = 0, scroll, i, lastedt, oldstr, newstr;

   tmp = this.ControlByHandle(sender);
   if   (tmp.Name == "BtnSave")
     this.DataSource.UpdateSerializers();
     if (this.DataSource.SerializeDown(this) AND this.DataSource.IsValid())
      retval = 1;
     else
      retval = 0;
     end;
   elif (tmp.Name == "BtnQuit")
     this.DataSource.UpdateSerializers();
     oldstr ="";
     newstr  ="";
     idx = this.DataSource.ErrorStream;
     this.DataSource.ErrorStream = TMTErrorStreamSilent();
     this.DataSource.SerializeDown(oldstr, WITH_FIELD);
     this.DataSource.SerializeDown(this);
     this.DataSource.SerializeDown(newstr, WITH_FIELD);
     if ((oldstr != newstr) AND VclGetTrue ("Документ изменен. Сохранить ?"))
       retval = 1;
     else
      this.DataSource.SerializeUp(FIELD_STRING_SPLITTER + oldstr, WITH_FIELD);
      retval =  2;
     end;
     this.DataSource.ErrorStream = idx;
   elif (tmp.Name == "BtnAdd")
     idx = this.AddMenu.Show ();
     if (idx != -1)
       scroll = this.ControlByName ("MainScroll");
       if ((this.AddMenu.Strings.Value(idx) != "F13C") AND
          (this.AddMenu.Strings.Value(idx) != "F23E"))
        if  ((FldNumFromName(this.AddMenu.Strings.Value(idx)) > 49) AND
             (FldNumFromName(this.AddMenu.Strings.Value(idx)) < 59))
          AddFld5X (this.AddMenu.Strings.Value(idx), "","","");
        elif(this.AddMenu.Strings.Value(idx) == "F13D")
          AddFld13D(this.AddMenu.Strings.Value(idx), "");
        elif(this.AddMenu.Strings.Value(idx) == "F26T")
          AddFld26T(idx, "");
        elif(this.AddMenu.Strings.Value(idx) == "F33B")
          AddFld33B (this.AddMenu.Strings.Value(idx), "")
        elif(this.AddMenu.Strings.Value(idx) == "F36")
          AddFld36 (this.AddMenu.Strings.Value(idx), "");
        elif(this.AddMenu.Strings.Value(idx) == "F70")
          AddFld70 (this.AddMenu.Strings.Value(idx), "");
        elif(this.AddMenu.Strings.Value(idx) == "F71F")
          AddRepeatableField (this.AddMenu.Strings.Value(idx), "", 60, 240, 130, 18,scroll.ControlByName ("F71A"), 3, true);
          RemoveFromAddMenu (this.AddMenu.Strings.Value(idx));
        elif(this.AddMenu.Strings.Value(idx) == "F71G")
          AddFld71G (this.AddMenu.Strings.Value(idx), "");
        elif(this.AddMenu.Strings.Value(idx) == "F72")
          AddFld72 (this.AddMenu.Strings.Value(idx), "");
        elif(this.AddMenu.Strings.Value(idx) == "F77B")
          AddFld77B (this.AddMenu.Strings.Value(idx), "");
        elif(this.AddMenu.Strings.Value(idx) == "F77T")
          AddFld77T (this.AddMenu.Strings.Value(idx), "");
        end;
       elif (this.AddMenu.Strings.Value(idx) == "F13C")
         AddRepeatableField (this.AddMenu.Strings.Value(idx),"", 60, 200, 130, 19, scroll.ControlByName ("F20"), 0);
       elif (this.AddMenu.Strings.Value(idx) == "F23E")
         AddRepeatableField (this.AddMenu.Strings.Value(idx),"", 60, 250, 230, 35, scroll.ControlByName ("F23B"), 0);
       end;
     end;
   elif (tmp.Name == "BtnRemove")
     idx = this.RemMenu.Show ();
     if (idx != -1)
       if ((this.RemMenu.Strings.Value(idx) == "F13C") OR
           (this.RemMenu.Strings.Value(idx) == "F23E") OR
           (this.RemMenu.Strings.Value(idx) == "F71F"))

        tmp = this.ControlByName ("Scroll" + this.RemMenu.Strings.Value(idx));
        this.RemoveControl (tmp.Controls.Value(tmp.Controls.Size - 1));
        if (this.RemMenu.Strings.Value(idx) == "F71F")
           this.AddMenu.AddString ("F71F");
           this.AddMenu.Sort();
        end;
        if (ValType(this.ControlByName (this.RemMenu.Strings.Value(idx)))== V_INTEGER)
         this.MoveFieldsUp (this.ControlByName ("MainScroll"),
                            tmp, tmp.GetHeight());
         this.RemoveControl (tmp.Name);
         this.RemMenu.RemString (idx);
        elif (this.RemMenu.Strings.Value(idx) == "F71F")
          this.RemMenu.RemString (idx);
        end;

       else
         tmp = this.ControlByName (this.RemMenu.Strings.Value(idx));
         this.MoveFieldsUp (this.ControlByName ("MainScroll"),
                            this.ControlByName (this.RemMenu.Strings.Value(idx)),
                            tmp.GetHeight());
         this.RemoveControl (this.RemMenu.Strings.Value(idx));
         this.AddMenu.AddString (this.RemMenu.Strings.Value(idx));
         this.AddMenu.Sort();
         this.RemMenu.RemString (idx);
       end;
     end;
   end;
   return retval;
 END;

 MACRO OnFocusRemove (sender)
  return 0;
 END;

 MACRO OnLkpButtonClick (sender)
  var currEdit = this.ControlByHandle (sender),
      num = int(FldNumFromName(currEdit.Name)),
      LookupFrm;
  if ((num >= 50) AND (num <=59))
    LookupFrm = TLkpFormF5X (currEdit, this.datasource.GetField(string(num)));
    LookupFrm.Create();
    LookupFrm.ShowModal();
  elif (num == 32)
    LookupFrm = TLkpFormF32 (currEdit);
    LookupFrm.Create();
    LookupFrm.ShowModal();
  else
    LookupFrm = TLkpFormFIIDSum (currEdit);
    LookupFrm.Create();
    LookupFrm.ShowModal();
  end;
  return 0;
 END;

 MACRO CreateFormAndScroll (_Name_, _Width_, _Height_)
   var tmp, Scroll;
   this.Top     = 0;
   this.Left    = 0;
   this.Width   = _Width_;
   this.Height  = _Height_;
   this.Handle  = ScrCreate(this.Top, this.Left, this.Height,
                       this.Width, _Name_, "DefFormProc");

   Scroll  = this.AddControl (C_SCROLLBOX, "MainScroll" , 1, 1, this.Height - 70, this.Width - 10);
   tmp = this.AddControl(C_BUTTON,"BtnSave", Scroll.GetTop() + Scroll.GetHeight() + 5,  Scroll.GetLeft() + 20, 25, 80, "Сохранить", 0);
   tmp = this.AddControl(C_BUTTON,"BtnQuit", tmp.GetTop(), tmp.GetLeft() + tmp.GetWidth() + 20, 25, 80,"Выйти", 0);
   tmp = this.AddControl(C_BUTTON,"BtnAdd", tmp.GetTop(), tmp.GetLeft() + tmp.GetWidth() + 20, 25, 85, "Добавить поле",0);
   this.AddControl(C_BUTTON,"BtnRemove", tmp.GetTop(), tmp.GetLeft() + tmp.GetWidth() + 20, 25, 85, "Удалить поле", 0);
 END;

 MACRO Create (_datasource)
   var scroll, tmp, i;

   CreateFormAndScroll("MT103", 430, 590);
   scroll = this.ControlByName("MainScroll");
   tmp = Scroll.AddControl(C_EDIT,"F20", 10, 70, 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT, "F23B", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft(), 0, 50, "" , ED_STRING, 0, 4);
   tmp = Scroll.AddControl(C_EDIT2SEC, "F32", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft(), 170, "A","", 3, 24);

   tmp = Scroll.AddControl(C_EDIT3SEC, "F50", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft() - 25, 50, 250, "","", "", 3, 0);

   tmp = Scroll.AddControl(C_EDIT3SEC, "F59", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft(), 50, 250, "","", "", 3, 0);
   tmp = Scroll.AddControl(C_EDIT, "F71A", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft() + 25, 0, 40, "" , ED_STRING, 0, 3);
   Scroll.ControlByName("F32").ReadOnlySet();
   Scroll.ControlByName("F50").ReadOnlySet();
   Scroll.ControlByName("F59").ReadOnlySet();
   this.AddMenu = TGUIMenu (NULL, "Добавить поле", 300, 250);
   this.RemMenu = TGUIMenu (NULL, "Удалить поле",  300, 250);

   this.AddMenu.AddString ("F13C");
   this.AddMenu.AddString ("F23E");
   this.AddMenu.AddString ("F26T");
   this.AddMenu.AddString ("F33B");
   this.AddMenu.AddString ("F36");
   i = 1;
   while (i < 8)
    this.AddMenu.AddString ("F5"+ string(i));
    i = i + 1;
   end;
   this.AddMenu.AddString ("F70");
   this.AddMenu.AddString ("F71F");
   this.AddMenu.AddString ("F71G");
   this.AddMenu.AddString ("F72");
   this.AddMenu.AddString ("F77B");
   this.AddMenu.AddString ("F77T");
   if (Valtype (_datasource) == V_GENOBJ)
     this.DataSource = _datasource;
     this.DataSource.UpdateSerializers();
     this.DataSource.SerializeUp(this);
     this.DataSource.ErrorStream = TMTErrorStreamVclMsgBox();
   end;
 END;

 InitTGUIform();
END;

CLASS (TGUIForm103) TGUIForm100

 MACRO OnLkpButtonClick (sender)
  var currEdit = this.ControlByHandle (sender),
      num = int(FldNumFromName(currEdit.Name)),
      LookupFrm;
  if ((num >= 50) AND (num <=59))
    LookupFrm = TLkpFormF5X (currEdit, this.datasource.GetField(string(num)));
    LookupFrm.Create();
    if (num == 50 )
     LookupFrm.ControlByName ("BIC").Disable();
     LookupFrm.ControlByName ("PI").Disable();
    end;
    if (num == 59 )
     LookupFrm.ControlByName ("BIC").Disable();
    end;
    LookupFrm.ShowModal();
  elif (num == 32)
    LookupFrm = TLkpFormF32 (currEdit);
    LookupFrm.Create();
    LookupFrm.ShowModal();
  end;
  return 0;
 END;

 MACRO AddFld71A (Name, Text)
   var tmp, scroll;

   scroll = this.ControlByName ("MainScroll");
   tmp = this.ControlByName ("F70");
   if (ValType(tmp) == V_INTEGER)
     tmp = this.ControlByName ("F59");
   end;
   this.MoveFieldsDown (scroll, tmp, this.ControlByName ("F20").GetHeight());
   Scroll.AddControl(C_EDIT, Name,
                     tmp.GetTop() + tmp.GetHeight() + YDist,
                     tmp.GetLeft(), 0, 40, Text , ED_STRING, 0, 3);
   RemoveFromAddMenu (Name);
 END;

 MACRO Create (_datasource)
  var scroll, tmp, i;
   CreateFormAndScroll("MT100", 430, 590);
   scroll = this.ControlByName("MainScroll");
   tmp = Scroll.AddControl(C_EDIT,"F20", 10, 70, 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT2SEC, "F32", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft(), 170, "A","", 3, 24);
   tmp = Scroll.AddControl(C_EDIT3SEC, "F50", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft() - 25, 50, 250, "","", "", 3, 0);
   tmp = Scroll.AddControl(C_EDIT3SEC, "F59", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft(), 50, 250, "","", "", 3, 0);
   Scroll.ControlByName("F32").ReadOnlySet();
   Scroll.ControlByName("F50").ReadOnlySet();
   Scroll.ControlByName("F59").ReadOnlySet();
   this.AddMenu = TGUIMenu (NULL, "Добавить поле", 300, 250);
   this.RemMenu = TGUIMenu (NULL, "Удалить поле",  300, 250);
   i = 2;
   while (i < 5)
    this.AddMenu.AddString ("F5"+ string(i));
    i = i + 1;
   end;
   this.AddMenu.AddString ("F56");
   this.AddMenu.AddString ("F57");
   this.AddMenu.AddString ("F70");
   this.AddMenu.AddString ("F71A");
   this.AddMenu.AddString ("F72");
   if (Valtype (_datasource) == V_GENOBJ)
     this.DataSource = _datasource;
     this.DataSource.UpdateSerializers();
     this.DataSource.SerializeUp(this);
     this.DataSource.ErrorStream = TMTErrorStreamVclMsgBox();
   end;

 END;

 InitTGUIform103();
END;

CLASS (TGUIForm103) TGUIForm200

 MACRO AddFld5X (Name, Opt, Pi, Na)
   var tmp, scroll, i, lastedt;

   if (Name == "F57")
    this.ControlByName ("F57").SetText(Opt,Pi,NA);
    return;
   end;
   scroll = this.ControlByName ("MainScroll");
   tmp = scroll.ControlByName ("F32");
   i = int(FldNumFromName (Name)) - 1;
   while (i > 52)
     lastedt = scroll.ControlByName ("F" + string(i));
     if (Valtype (lastedt) != V_INTEGER)
      tmp = lastedt;
      i = 52;
     end;
     i = i - 1;
   end;
   this.MoveFieldsDown (scroll, tmp, 75);
   tmp = Scroll.AddControl(C_EDIT3SEC, Name,
                            tmp.GetTop() + tmp.GetHeight() + YDist,
                            scroll.ControlByName("F57").GetLeft(), 50, 250, Opt, Pi, Na, 3, 0);
   tmp.ReadOnlySet();
   RemoveFromAddMenu (Name);
 END;

 MACRO AddFld72 (Name, Text)
  var tmp, scroll;

  tmp = this.ControlByName ("F57");
  scroll = this.ControlByName ("MainScroll");
  this.MoveFieldsDown (scroll, tmp, 58);
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    58, 250, Text, ED_STRING, 0, 140);
  RemoveFromAddMenu (Name);
 END;

 MACRO Create (_datasource)
  var scroll, tmp, i;
   CreateFormAndScroll("MT200", 430, 590);
   scroll = this.ControlByName("MainScroll");
   tmp = Scroll.AddControl(C_EDIT,"F20", 10, 70, 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT2SEC, "F32", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft(), 170, "A","", 3, 24);
   tmp = Scroll.AddControl(C_EDIT3SEC, "F57", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft() - 25, 50, 250, "","", "", 3, 0);
   Scroll.ControlByName("F57").ReadOnlySet();
   Scroll.ControlByName("F32").ReadOnlySet();
   this.AddMenu = TGUIMenu (NULL, "Добавить поле", 300, 250);
   this.RemMenu = TGUIMenu (NULL, "Удалить поле",  300, 250);
   this.AddMenu.AddString ("F53");
   this.AddMenu.AddString ("F56");
   this.AddMenu.AddString ("F72");
   if (Valtype (_datasource) == V_GENOBJ)
     this.DataSource = _datasource;
     this.DataSource.UpdateSerializers();
     this.DataSource.SerializeUp(this);
     this.DataSource.ErrorStream = TMTErrorStreamVclMsgBox();
   end;

 END;

 InitTGUIform103();
END;

CLASS (TGUIForm103) TGUIForm202

 MACRO AddFld5X (Name, Opt, Pi, Na)
   var tmp, scroll, i, lastedt;

   if (Name == "F58")
    this.ControlByName ("F58").SetText(Opt,Pi,NA);
    return;
   end;
   scroll = this.ControlByName ("MainScroll");
   tmp = scroll.ControlByName ("F32");
   i = int(FldNumFromName (Name)) - 1;
   while (i > 52)
     lastedt = scroll.ControlByName ("F" + string(i));
     if (Valtype (lastedt) != V_INTEGER)
      tmp = lastedt;
      i = 52;
     end;
     i = i - 1;
   end;
   this.MoveFieldsDown (scroll, tmp, 75);
   tmp = Scroll.AddControl(C_EDIT3SEC, Name,
                            tmp.GetTop() + tmp.GetHeight() + YDist,
                            scroll.ControlByName("F58").GetLeft(), 50, 250, Opt, Pi, Na, 3, 0);
   tmp.ReadOnlySet();
   RemoveFromAddMenu (Name);
 END;

 MACRO AddFld72 (Name, Text)
  var tmp, scroll;

  tmp = this.ControlByName ("F58");
  scroll = this.ControlByName ("MainScroll");
  this.MoveFieldsDown (scroll, tmp, 58);
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    58, 250, Text, ED_STRING, 0, 140);
  RemoveFromAddMenu (Name);
 END;

 MACRO Create (_datasource)
  var scroll, tmp, i;
   CreateFormAndScroll("MT202", 430, 590);
   scroll = this.ControlByName("MainScroll");
   tmp = Scroll.AddControl(C_EDIT,"F20", 10, 70, 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT,"F21", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT2SEC, "F32", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 170, "A","", 3, 24);
   tmp = Scroll.AddControl(C_EDIT3SEC, "F58", tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft() - 25, 50, 250, "","", "", 3, 0);
   Scroll.ControlByName("F58").ReadOnlySet();
   Scroll.ControlByName("F32").ReadOnlySet();
   this.AddMenu = TGUIMenu (NULL, "Добавить поле", 300, 250);
   this.RemMenu = TGUIMenu (NULL, "Удалить поле",  300, 250);

   /*this.AddMenu.AddString ("F13C");*/
   i = 2;
   while (i < 8)
    if (i != 5)
     this.AddMenu.AddString ("F5"+ string(i));
    end;
    i = i + 1;
   end;
   this.AddMenu.AddString ("F72");
   if (Valtype (_datasource) == V_GENOBJ)
     this.DataSource = _datasource;
     this.DataSource.UpdateSerializers();
     this.DataSource.SerializeUp(this);
     this.DataSource.ErrorStream = TMTErrorStreamVclMsgBox();
   end;

 END;

 InitTGUIform103();
END;

CLASS (TGUIForm103) TGUIForm900

 MACRO AddFld5X (Name, Opt, Pi, Na)
   var tmp, scroll, i, lastedt;
   scroll = this.ControlByName ("MainScroll");
   tmp = scroll.ControlByName ("F32");
   this.MoveFieldsDown (scroll, tmp, 75);
   tmp = Scroll.AddControl(C_EDIT3SEC, Name,
                            tmp.GetTop() + tmp.GetHeight() + YDist,
                            tmp.GetLeft() - 25, 50, 250, Opt, Pi, Na, 3, 0);
   tmp.ReadOnlySet();
   RemoveFromAddMenu (Name);
 END;

 MACRO AddFld72 (Name, Text)
  var tmp, scroll;
  tmp = this.ControlByName ("F52");
  if (valtype (tmp) == V_INTEGER)
   tmp = this.ControlByName ("F32");
  end;
  scroll = this.ControlByName ("MainScroll");
  this.MoveFieldsDown (scroll, tmp, 58);
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    58, 250, Text, ED_STRING, 0, 140);
  RemoveFromAddMenu (Name);
 END;

 MACRO Create (_datasource)
  var scroll, tmp, i;
   CreateFormAndScroll("MT900", 430, 490);
   scroll = this.ControlByName("MainScroll");
   tmp = Scroll.AddControl(C_EDIT,"F20", 10, 70, 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT,"F21", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT,"F25", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 0, 250, "" , ED_STRING, 0, 35);
   tmp = Scroll.AddControl(C_EDIT2SEC, "F32", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 170, "A","", 3, 24);
   Scroll.ControlByName("F32").ReadOnlySet();
   this.AddMenu = TGUIMenu (NULL, "Добавить поле", 300, 250);
   this.RemMenu = TGUIMenu (NULL, "Удалить поле",  300, 250);
   this.AddMenu.AddString ("F13D");
   this.AddMenu.AddString ("F52");
   this.AddMenu.AddString ("F72");
   if (Valtype (_datasource) == V_GENOBJ)
     this.DataSource = _datasource;
     this.DataSource.UpdateSerializers();
     this.DataSource.SerializeUp(this);
     this.DataSource.ErrorStream = TMTErrorStreamVclMsgBox();
   end;

 END;
 InitTGUIform103();
END;

CLASS (TGUIForm103) TGUIForm910

 MACRO AddFld5X (Name, Opt, Pi, Na)
   var tmp, scroll, i, lastedt;

   scroll = this.ControlByName ("MainScroll");
   tmp = scroll.ControlByName ("F32");
   i = int(FldNumFromName (Name)) - 1;
   while (i > 49)
     lastedt = scroll.ControlByName ("F" + string(i));
     if (Valtype (lastedt) != V_INTEGER)
      tmp = lastedt;
      i = 49;
     end;
     i = i - 1;
   end;
   this.MoveFieldsDown (scroll, tmp, 75);
   tmp = Scroll.AddControl(C_EDIT3SEC, Name,
                            tmp.GetTop() + tmp.GetHeight() + YDist,
                            scroll.ControlByName("F32").GetLeft() - 25,
                            50, 250, Opt, Pi, Na, 3, 0);
   tmp.ReadOnlySet();
   RemoveFromAddMenu (Name);
 END;

 MACRO AddFld72 (Name, Text)
  var tmp, scroll;

  tmp = this.ControlByName ("F56");
  if (valtype (tmp) == V_INTEGER)
   tmp = this.ControlByName ("F52");
  end;
  if (valtype (tmp) == V_INTEGER)
   tmp = this.ControlByName ("F50");
  end;
  if (valtype (tmp) == V_INTEGER)
   tmp = this.ControlByName ("F32");
  end;
  scroll = this.ControlByName ("MainScroll");
  this.MoveFieldsDown (scroll, tmp, 58);
  Scroll.AddControl(C_EDIT, Name,
                    tmp.GetTop() + tmp.GetHeight() + YDist,
                    this.ControlByName ("F20").GetLeft(),
                    58, 250, Text, ED_STRING, 0, 140);
  RemoveFromAddMenu (Name);
 END;

 MACRO Create (_datasource)
  var scroll, tmp, i;
   CreateFormAndScroll("MT910", 430, 490);
   scroll = this.ControlByName("MainScroll");
   tmp = Scroll.AddControl(C_EDIT,"F20", 10, 70, 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT,"F21", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 0, 130, "" , ED_STRING, 0, 16);
   tmp = Scroll.AddControl(C_EDIT,"F25", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 0, 250, "" , ED_STRING, 0, 35);
   tmp = Scroll.AddControl(C_EDIT2SEC, "F32", tmp.GetTop() + tmp.GetHeight() + YDist,
                           tmp.GetLeft(), 170, "A","", 3, 24);
   Scroll.ControlByName("F32").ReadOnlySet();
   this.AddMenu = TGUIMenu (NULL, "Добавить поле", 300, 250);
   this.RemMenu = TGUIMenu (NULL, "Удалить поле",  300, 250);

   this.AddMenu.AddString ("F13D");
   this.AddMenu.AddString ("F50");
   this.AddMenu.AddString ("F52");
   this.AddMenu.AddString ("F56");
   this.AddMenu.AddString ("F72");
   if (Valtype (_datasource) == V_GENOBJ)
     this.DataSource = _datasource;
     this.DataSource.UpdateSerializers();
     this.DataSource.SerializeUp(this);
     this.DataSource.ErrorStream = TMTErrorStreamVclMsgBox();
   end;

 END;

 InitTGUIform103();
END;
