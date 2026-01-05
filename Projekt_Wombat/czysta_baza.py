import sqlalchemy
from sqlalchemy import create_engine,text
from pliki.warunki_bazy import *
from pliki.tworzenie import *


try:
    engine = sqlalchemy.create_engine("mariadb+mariadbconnector://team06:te%40mzaog@giniewicz.it:3306/team06")   
    con=engine.connect() 
except:
    print("Nie można nawiązać połączenia z bazą")
    raise SystemExit
print("Poprawnie nawiązano połączenie z bazą.")

try:
    clearing_base(engine)
except:
    print("Błąd przy czyszczeniu dotychczasowej bazy, nie można wykonać programu.")
    raise SystemExit
print("Baza poprawnie wyczyszczona.")

fi="pliki\struktura.json"
try:
    creating_base(engine,fi)
except:
    print("Błąd przy tworzeniu struktury bazy, nie można wykonać programu.")
    raise SystemExit
print("Baza poprawnie utworzona.")

na="dodanie_pracowników"
tab="wyjazdy"
do="""
IF NOT EXISTS (SELECT * FROM pracownicy INNER JOIN stanowiska USING (id_stanowiska) 
        WHERE (stanowiska.stanowisko="przewodnik" AND pracownicy.id_pracownika=NEW.id_przewodnika)) THEN
    SIGNAL SQLSTATE '45000' 
    SET MESSAGE_TEXT = 'Nie można dodać przewodnika niepracującego na odpowiednim stanowisku';
ELSEIF NOT EXISTS (SELECT * FROM pracownicy INNER JOIN stanowiska USING (id_stanowiska) 
        WHERE (stanowiska.stanowisko="agent" AND pracownicy.id_pracownika=NEW.id_agenta)) THEN
    SIGNAL SQLSTATE '45000' 
    SET MESSAGE_TEXT = 'Nie można dodać agenta niepracującego na odpowiednim stanowisku';
END IF;
"""
try:
    creating_triggers(engine,na,tab,do)
except:
    print("Błąd przy tworzeniu warunków w bazie, nie można wykonać programu.")
    raise SystemExit
print("Wyzwalacze poprawnie dodane.")