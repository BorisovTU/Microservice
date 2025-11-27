macro GetEmptyValue(ValueType):variant;
 if (ValueType==V_INTEGER)
   return Int(0);
 elif (ValueType==V_DOUBLE)
   return Double(0);
 elif (ValueType==V_DOUBLEL)
   return DoubleL(0);
 elif (ValueType==V_MONEY)
   return double(0);
 elif (ValueType==V_MONEYL)
   return doublel(0);
 elif (ValueType==V_DATE)
   return Date(0,0,0);
 elif (ValueType==V_TIME)
   return Time(0,0,0);
 elif (ValueType==V_STRING)
   return "";
 else
   return null;
 end;
end;

