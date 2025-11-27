
CREATE OR REPLACE TYPE debt_row_type AS OBJECT (
    currency  NUMBER,
    debtVal   NUMBER,
    dlcontrid NUMBER
)
/

CREATE OR REPLACE TYPE debt_table AS TABLE OF debt_row_type
/