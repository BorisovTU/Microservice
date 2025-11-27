/*зазор между именем поля и самим полем */
const LabelGap = 5;
/*типы данных для полей ввода */
const ED_STRING = 1, ED_SHORT = 2, ED_LONG = 3, ED_MONEY = 4,
      ED_DATE = 5, ED_TIME = 6, ED_FLOAT = 7;

CLASS TEdit ()
 var Name,
     Handle,
     Text,
     MaxLen,
     BtnType,
     Mode,
     Enabled,
     Parent,
     NameLabel,
     NameIsUp,
     NameHidden = false,
     MainForm,
     ReadOnly = false,
     Top = 0, Left = 0, Width = 0, Height = 0;

 MACRO SetText (val)
  if (this.Handle)
   this.Text = trim(val);
   ScrSetText (this.Handle, this.Text);
  end;
 END;


 MACRO MyHandle (hndl)
    return (hndl == this.Handle )
 END;

 MACRO GetText ()
    if (this.Handle)
     this.Text = ScrGetText (this.Handle);
    else
      this.Text =  "";
    end;
    return this.Text;
 END;

 MACRO Hide ()
   if (this.Handle)
     ScrHide (this.Handle);
   end;
 END;

 MACRO Show ()
   if (this.Handle)
     ScrShow (this.Handle);
   end;
 END;

 MACRO Enable ()
    if (this.Handle)
      Enabled = true;
      ScrEnable (this.Handle, true);
    end;
 END;

 MACRO Disable ()
    if (this.Handle)
      Enabled = false;
      ScrEnable (this.Handle, false);
    end;
 END;

 MACRO ReadOnlySet()
    if (this.Handle)
      ScrReadOnly (this.Handle, true);
      ReadOnly = true;
    end;
 END;

 MACRO ReadOnlyRem()
    if (this.Handle)
      ScrReadOnly (this.Handle, false);
      ReadOnly = false;
    end;
 END;

 MACRO IsReadOnly ()
   return this.ReadOnly;
 END;

 MACRO ShowName (Up)
    var coeff = 6.5;
   if (this.Handle AND (strlen (this.Name)))
     if (not this.NameLabel)
      if (Up)
       this.NameIsUp = true;
       this.NameLabel = ScrAddLabel (Parent, this.Top - LabelGap, this.Left, this.Name);
      else
       this.NameIsUp = false;
       this.NameLabel = ScrAddLabel (Parent, this.Top, this.Left - int(coeff *strlen(this.Name))-
                                     LabelGap, this.Name);
      end;
     else
      ScrShow (this.NameLabel);
     end;
     this.NameHidden = false;
   end;
 END;

 MACRO HideName ()
   if (this.NameLabel)
     ScrHide (this.NameLabel);
     this.NameHidden = true;
   end;
 END;

 MACRO Clear ()
    SetText ("");
 END;

 MACRO Remove ()
    this.Clear();
    if (this.Handle)
      ScrDelete (this.Handle);
    end;
    this.Handle = 0;
    if (this.NameLabel)
      ScrDelete (this.NameLabel);
    end;
    this.NameLabel = 0;
 END;

 MACRO Move (NewX, NewY)

   if (this.Handle AND ((NewX != this.Left) OR (NewY != this.Top)))
      this.Text = this.GetText ();
      ScrDelete (this.Handle);
      this.Top  = NewY;
      this.Left = NewX;
      this.Handle = ScrAddEdit (this.Parent, this.Top, this.Left, this.Height, this.Width, this.Text, this.Mode, this.MainForm, this.BtnType, this.MaxLen);
      if (this.NameLabel)
       ScrDelete (this.NameLabel);
       this.NameLabel = 0;
       this.ShowName (this.NameIsUp);
      end;
      if (this.ReadOnly)
        this.ReadOnlySet();
      end;
      if (not this.Enabled)
        this.Disable();
      end;
   end;
   return (this.Handle == 0);
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

 MACRO Create (Name, cnt, Top, Left, Height, Width, Text, Mode, scr, btn ,MaxLen)
   this.MainForm = NULL;
   this.Parent = cnt;
   this.Top    = Top;
   this.Left   = Left;
   this.Height = Height;
   this.Width  = Width;
   this.Text   = trim(Text);
   this.Mode   = Mode;
   this.Name   = trim(Name);
   if (ValType (btn) != V_UNDEF)
    this.BtnType   = btn;
   else
    this.BtnType   = NULL;
   end;
   if (ValType (MaxLen) != V_UNDEF)
    this.MaxLen   = MaxLen;
   else
    this.MaxLen   = NULL;
   end;
   if (ValType (scr)!= V_UNDEF )
     this.MainForm = scr;
   end;
   this.Handle = ScrAddEdit (this.Parent, this.Top, this.Left, this.Height, this.Width,
                             this.Text, this.Mode, this.MainForm, this.BtnType, this.MaxLen);
   this.ShowName (false);
   return (this.Handle == 0);
 END;
 NameIsUp  = false;
 Enabled   = true;
 NameLabel = 0;
 Handle    = 0;
 Parent    = 0;
END;

CLASS TButton ()
 var Name,
     Handle,
     Type,   /* значение возвращаемое из scrProc */
     Enabled,
     Parent,
     MainForm,
     Text,
     Top = 0, Left = 0, Width = 0, Height = 0;

  MACRO Create (Name, cnt, Top, Left, Height, Width, Text, Tag, scr)
   this.MainForm = NULL;
   this.Parent = cnt;
   this.Top    = Top;
   this.Left   = Left;
   this.Height = Height;
   this.Width  = Width;
   this.Type   = Tag;
   this.Name   = trim(Name);
   this.Text   = trim(Text);
   if (ValType (scr)!= V_UNDEF )
     this.MainForm = scr;
   end;
   this.Handle = ScrAddButton (this.Parent, this.Top, this.Left, this.Height, this.Width, this.Text, this.Type, this.MainForm);
  END;

  MACRO MyHandle (hndl)
    return (hndl == this.Handle)
  END;

  MACRO Remove ()
    if (this.Handle)
      ScrDelete (this.Handle);
    end;
    this.Handle = 0;
  END;

  MACRO GetTop ()
   return this.Top;
  END;

  MACRO GetLeft ()
   return this.Left;
  END;

  MACRO GetHeight ()
   return this.Height;
  END;

  MACRO GetWidth ()
   return this.Width;
  END;

  MACRO Hide ()
   if (this.Handle)
     ScrHide (this.Handle);
   end;
  END;

  MACRO Move (newX, newY)
   if (this.Handle AND ((NewX != this.Left) OR (NewY != this.Top)))
      ScrDelete (this.Handle);
      this.Top  = NewY;
      this.Left = NewX;
      this.Handle = ScrAddButton (this.Parent, this.Top, this.Left, this.Height, this.Width, this.Text, this.Type, this.MainForm);
   end;
   return (this.Handle == 0);
  END;

  Handle  = 0;
  Name    = "";
  Enabled = true;
  Parent  = 0;
END;

CLASS TEdit2Sec ()

  var EditSec1 = NULL, EditSec2 = NULL, Mode  = ED_STRING, MaxLen1 = 1, Name, Handle;

  MACRO Create  (Name, cnt, Top, Left, Width, Opt, Text, scr, btn, MaxLen)

    EditSec1 = TEdit ();
    EditSec2 = TEdit ();
    this.Name = trim(Name);
    EditSec1.Create (Name, cnt, Top, Left, 0, 20, Opt, this.Mode, scr, 0, MaxLen1);
    this.Handle = EditSec1.Handle;
    EditSec2.Create ("", cnt, Top, Left + EditSec1.GetWidth()+ labelgap , 0, Width, Text, this.Mode, scr, btn, MaxLen);
  END;

  MACRO MyHandle (hndl)
    return ((hndl == EditSec2.Handle) OR
            (hndl == EditSec1.Handle))
  END;

  MACRO GetText (type)
    var retval;

    if (type == 0)/* опция  */
     retval =  EditSec1.GetText ();
    elif (type == 1) /* значение */
     retval =  EditSec2.GetText ();
    elif (type == 2)
     retval =  EditSec1.GetText () + ":" + EditSec2.GetText ();
    end;

    return retval;
  END;

  MACRO SetText (opt, val)
     EditSec1.SetText (opt);
     EditSec2.SetText (val);
  END;

  MACRO GetTop ()
   return EditSec1.GetTop();
  END;

  MACRO GetLeft ()
   return EditSec1.GetLeft();
  END;

  MACRO GetHeight ()
   return EditSec1.GetHeight();
  END;

  MACRO GetWidth ()
   return (EditSec1.GetWidth() + labelgap + EditSec2.GetWidth());
  END;

  MACRO Hide ()
    this.EditSec1.Hide();
    this.EditSec2.Hide();
  END;

  MACRO Show ()
    this.EditSec1.Show();
    this.EditSec2.Show();
  END;

  MACRO ReadOnlySet()
     this.EditSec1.ReadOnlySet();
     this.EditSec2.ReadOnlySet();
  END;

  MACRO IsReadOnly ()
   return (this.EditSec1.ReadOnly AND this.EditSec2.ReadOnly);
  END;

  MACRO ReadOnlyRem()
    this.EditSec1.ReadOnlyRem();
    this.EditSec2.ReadOnlyRem();
  END;

  MACRO Enable()
     this.EditSec1.Enable();
     this.EditSec2.Enable();
  END;

  MACRO Disable()
    this.EditSec1.Disable();
    this.EditSec2.Disable();
  END;

  MACRO Remove ()
    EditSec1.Remove ();
    EditSec2.Remove ();
  END;

  MACRO Move (newx, newy)
    EditSec1.Move (newx, newy);
    EditSec2.Move (newx + EditSec1.GetWidth() + LabelGap, newy);
  END;

END;

CLASS TEdit3Sec ()

  var EditSec1 = NULL, EditSec2 = NULL, EditSec3 = NULL, Name, Handle,
      MaxLen1 = 1, MaxLen2 = 37, MaxLen3 = 146, Mode = ED_STRING;

  MACRO Create  (Name, cnt, Top, Left, Height, Width, Opt, Pi, Text, scr, btn, MaxLen)

    EditSec1 = TEdit ();
    EditSec2 = TEdit ();
    EditSec3 = TEdit ();
    this.Name = trim(Name);
    EditSec1.Create (Name, cnt, Top, Left, 0,  20, Opt, Mode, scr, 0, MaxLen1);
    this.Handle = EditSec1.Handle;
    EditSec2.Create ("", cnt, Top, Left+EditSec1.GetWidth()+ labelgap,   0,  Width, PI,  Mode, scr, btn, MaxLen2);
    EditSec3.Create ("", cnt, Top + EditSec1.GetHeight()+ labelgap, EditSec2.GetLeft(),  Height,  Width, Text,Mode, scr, 0, MaxLen3);
  END;

  MACRO MyHandle (hndl)
    return ((hndl == EditSec3.Handle) OR (hndl == EditSec2.Handle) OR
            (hndl == EditSec1.Handle))
  END;

  MACRO GetText (type)
    var retval;

    if (type == 0)   /* опция   */
     retval =  EditSec1.GetText ();
    elif (type == 1) /* счет  */
     retval =  EditSec2.GetText ();
    elif (type == 2) /* адрес и.т.д */
     retval =  EditSec3.GetText ();
    elif (type == 3)
     retval =  EditSec1.GetText () + ":" + EditSec2.GetText () +
               EditSec3.GetText ();
    end;

    return retval;
  END;

  MACRO SetText (opt, pi , val)
    EditSec1.SetText (opt);
    EditSec2.SetText (pi);
    EditSec3.SetText (val);
  END;

  MACRO GetTop ()
   return EditSec1.GetTop();
  END;

  MACRO GetLeft ()
   return EditSec1.GetLeft();
  END;

  MACRO GetHeight ()
   return (EditSec1.GetHeight + labelgap + EditSec3.GetHeight());
  END;

  MACRO GetWidth ()
   return (EditSec1.GetWidth + labelgap + EditSec3.GetWidth());
  END;

  MACRO Hide ()
    this.EditSec1.Hide();
    this.EditSec2.Hide();
    this.EditSec3.Hide();
  END;

  MACRO Show ()
    this.EditSec1.Show();
    this.EditSec2.Show();
    this.EditSec3.Show();
  END;

  MACRO ReadOnlySet()
     this.EditSec1.ReadOnlySet();
     this.EditSec2.ReadOnlySet();
     this.EditSec3.ReadOnlySet();
  END;

  MACRO ReadOnlyRem()
    this.EditSec1.ReadOnlyRem();
    this.EditSec2.ReadOnlyRem();
    this.EditSec3.ReadOnlyRem();
  END;

  MACRO IsReadOnly ()
   return (this.EditSec1.ReadOnly AND this.EditSec2.ReadOnly AND this.EditSec3.ReadOnly);
  END;

  MACRO Enable()
     this.EditSec1.Enable();
     this.EditSec2.Enable();
     this.EditSec3.Enable();
  END;

  MACRO Disable()
    this.EditSec1.Disable();
    this.EditSec2.Disable();
    this.EditSec3.Disable();
  END;

  MACRO Remove ()
    EditSec1.Remove ();
    EditSec2.Remove ();
    EditSec3.Remove ();
  END;

  MACRO Move (newx, newy)
    EditSec1.Move (newx, newy);
    EditSec2.Move (newx + editsec1.GetWidth() + LabelGap, newy);
    EditSec3.Move (newx + editsec1.GetWidth()+ LabelGap,
                   newy + editsec2.GetHeight() + labelgap);
  END;

END;


