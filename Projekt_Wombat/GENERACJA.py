import sqlalchemy
from sqlalchemy import create_engine, text, insert, MetaData
from datetime import datetime, timedelta, date
import random
from faker import Faker
import csv
import pandas as pd
engine = sqlalchemy.create_engine("mariadb+mariadbconnector://team06:te%40mzaog@giniewicz.it:3306/team06")   
con = engine.connect() 
metadata = MetaData() 
metadata.reflect(engine) 
faker_pl = Faker('pl_PL')
faker_default = Faker() 

def waluty(con, sciezka_csv):
        url = 'https://tavex.pl/kursy-walut/?officeId=67'
        page = pd.read_html(url)
        data = page[-1]
        data = data.iloc[: , : 2]
        for i in range(len(data)):
            name = data.iloc[i, 0]
            name = name[1: ]
            index = name.find(" ")
            data.iloc[i, 0] = name[index + 2: ]
            if name[-5: -1] == "1000":
                data.iloc[i, 0] = name[: -7]
                data.iloc[i, 1] /= 1000
        zl = pd.DataFrame([["Złoty", 1]], columns = data.columns)
        data = pd.concat([data, zl], ignore_index = True)

        currencies = pd.read_csv(sciezka_csv, header = None)
        currencies = currencies.iloc[: , 1]
        added_curr = set()

        waluty = metadata.tables.get("waluty")
        query = text("ALTER TABLE waluty AUTO_INCREMENT = 1")
        con.execute(query)

        for i in currencies:
            if i in added_curr:
                continue
            added_curr.add(i)

            index = data[data.iloc[:,  0] == i].index[0]
            query = insert(waluty).values(nazwa_waluty = data.iat[index, 0], kurs_na_zł = data.iat[index, 1], ostatnia_aktualizacja = datetime.now())
            con.execute(query)

        print("Waluty poprawnie dodane")

def kraje(con, sciezka_csv):
    df = pd.read_csv(sciezka_csv, header=None, names=['kraj', 'nazwa_waluty'])

    waluty = con.execute(text("SELECT id_waluty, nazwa_waluty FROM waluty")).fetchall()
    waluty_map = {row[1].lower(): row[0] for row in waluty}

    for _, row in df.iterrows():
        kraj = row['kraj']
        nazwa_waluty = row['nazwa_waluty'].lower()
        id_waluty = waluty_map.get(nazwa_waluty)

        try:
            query = """
                INSERT INTO kraje (kraj, id_waluty) 
                VALUES (:kraj, :id_waluty)
            """
            con.execute(text(query), {'kraj': kraj, 'id_waluty': id_waluty})
        except Exception as e:
            print(f"Błąd {e}")
    print("Wstawiono dane 'kraje'")


def grupy(con, num_records=15):
    try:
        typy_grup = ["Wycieczka szkolna", "Grupa seniorów", "Jednoosobowa wycieczka", "Grupa rodzinna", "Romantyczny wyjazd we dwoje", 
                     "Weekendowy wyjazd", "Pole namiotowe", "Jednodniowa wycieczka", "Międzynarodowe wakacje",
                     "Wycieczka krajoznawcza", "Wycieczka fotograficzna", "Wyjazd kulturowy", "Wyprawa survivalowa", "Wyjazd edukacyjny",
                     "Wyprawa historyczna"]
                     
        for _ in range(num_records):
            typ_grupy = random.choice(typy_grup)

            if typ_grupy in ["Wycieczka szkolna", "Jednoosobowa wycieczka", "Grupa seniorów"]:
                nazwa = None 
            elif typ_grupy in ["Wyprawa przygodowa"]:
                nazwa = random.choice(["Szlakiem dzikiej przyrody","Ekspedycja w nieznane", "Przygoda na krańcu świata", "Ahoj przygodo!"])
            elif typ_grupy in ["Wyjazd edukacyjny"]:
                nazwa = random.choice(["Nauka w podróży","Edukacyjna ekspedycja", "Nauka poprzez zabawę"])
            elif typ_grupy in ["Wyprawa historyczna"]:
                nazwa = random.choice(["Podróż przez wieki", "Śladami historii", "Zabawa historią","Ciekawostki historyczne", "Szlakiem historii"])
            elif typ_grupy in ["Wycieczka krajoznawcza", "Wyjazd kulturowy"]:
                nazwa =  random.choice(["Najpiękniejsze miasto świata", "Piękno zagranicznego krajobrazu", "Ciekawa zagraniczna kultura", "Ciekawostki kulturowe","Śladami zagrnaicznej kultury"])
            elif typ_grupy in ["Wyprawa survivalowa", "Ultra ekstremalna wycieczka"]:
                nazwa =  random.choice(["Szkoła przetrwania","Survival w dziczy","Test na wytrzymałość"])
            elif typ_grupy in ["Wycieczka fotograficzna"]:
                nazwa = random.choice(["Kadry z natury","Okiem obiektywu"])
            elif typ_grupy in ["Grupa seniorów"]:
                nazwa = "Spokojne zwiedzanie za granicą"
            elif typ_grupy in ["Grupa rodzinna"]:
                nazwa = random.choice(["Spokojne zwiedzanie za granicą", "Szalona rodzinna wycieczka", "Rozrywka dla każdego pokolenia", "Wielopokoleniowa rozrywka"])        
            else:
                nazwa = None

            query = "INSERT INTO grupy (typ_grupy, nazwa) VALUES (:typ_grupy, :nazwa)"
            con.execute(text(query), {'typ_grupy': typ_grupy, 'nazwa': nazwa})

        print(f"Wstawiono {num_records} rekordów do tabeli 'grupy'")
    except Exception as e:
        print(f"Error'grupy': {e}")

def stanowiska(con):
    try:
        stanowiska_lista = ["Przewodnik", "Przewodnik Pro", "Kierowca", "Recepcjonista", 
                            "Pracownik Infolinii", "Logistyk", "Kierownik wycieczki", "Pilot wycieczek", "Dyrektor", 
                            "HR", "Animator", "Koordynator", "Agent", "Manager", "Zastępca dyrektora", "Grafik", 
                            "Programista", "Konserwator", "Osoba sprzątająca", "Specjalista od rezerwacji", 
                            "Specjalista ds. social media", "Marketing", "Specjalista ds. BHP", "Specjalista ds. promocji"]

        for stanowisko in stanowiska_lista:
            id_waluty = 12 
            zarobek_netto = 3500
            dodatek_netto = 0  

            if stanowisko in ["Przewodnik Pro", "Dyrektor", "Zastępca dyrektora", "Manager", "Kierownik wycieczki"]:
                zarobek_netto = random.randint(5000, 7000)
            elif stanowisko in ["Przewodnik", "Kierowca", "Logistyk", "Kierownik wycieczki", "Pilot wycieczek",
                                "HR", "Koordynator", "Agent", "Grafik", "Programista", "Specjalista ds. social media",
                                "Marketing", "Specjalista ds. promocji"]:
                zarobek_netto = random.randint(4000, 5000)
            elif stanowisko in ["Konserwator", "Specjalista od rezerwacji", "Animator", "Specjalista ds. BHP",
                                "Recepcjonista", "Pracownik Infolinii", "Osoba sprzątająca"]:
                zarobek_netto = random.randint(3000, 4000)

            if stanowisko == "Przewodnik":
                dodatek_netto = 100
            elif stanowisko == "Przewodnik Pro":
                dodatek_netto = 800
            elif stanowisko == "Dyrektor":
                dodatek_netto = 1000
            elif stanowisko == "Agent":
                dodatek_netto = 500

            zarobek_brutto = zarobek_netto * 1.2
            dodatek_brutto = dodatek_netto * 1.2  

            if stanowisko in ["Dyrektor", "Zastępca dyrektora", "Agent"]:
                status_studenta = 0
            else: 
                status_studenta = random.choice([0, 1])
                if status_studenta == 1:
                    zarobek_brutto = zarobek_netto
                    dodatek_brutto = dodatek_netto
            
            query = text("""
                INSERT INTO stanowiska (stanowisko, zarobek_netto, zarobek_brutto, dodatek_netto, dodatek_brutto, id_waluty, status_studenta) 
                VALUES (:stanowisko, :zarobek_netto, :zarobek_brutto, :dodatek_netto, :dodatek_brutto, :id_waluty, :status_studenta)
            """)

            con.execute(query, {
                'stanowisko': stanowisko,
                'zarobek_netto': zarobek_netto,
                'zarobek_brutto': zarobek_brutto,
                'dodatek_netto': dodatek_netto,
                'dodatek_brutto': dodatek_brutto,
                'id_waluty': id_waluty,
                'status_studenta': status_studenta
            })

        print(f"Wstawiono {len(stanowiska_lista)} rekordów do tabeli 'stanowiska'")

    except Exception as e:
        print(f"Błąd 'stanowiska': {e}")

def firmy(con):
    try:
        kraje = con.execute(text("SELECT id_kraju, kraj FROM kraje")).fetchall()
        kraje_map = {row[1]: row[0] for row in kraje}

        for kraj, id_kraju in kraje_map.items():
            nazwa_firmy = faker_default.company()[:100]
            if kraj.lower() == "polska":
                NIP = faker_pl.unique.numerify(text="##########")
                nazwa_firmy = faker_pl.company()[:100]
            else:
                NIP = None

            data_podpisania = faker_default.date_between(start_date="-5y", end_date="today")
            data_obowiązywania = data_podpisania + timedelta(days=random.randint(30, 365*2))

            query = text(""" 
                INSERT INTO firmy (nazwa_firmy, id_kraju, NIP, data_podpisania_kontraktu, data_obowiązywania_kontraktu)
                VALUES (:nazwa_firmy, :id_kraju, :NIP, :data_podpisania, :data_obowiązywania)
            """)
            con.execute(query, {
                "nazwa_firmy": nazwa_firmy,
                "id_kraju": id_kraju,
                "NIP": NIP,
                "data_podpisania": data_podpisania,
                "data_obowiązywania": data_obowiązywania
            })

        print(f"Wstawiono {len(kraje_map)} rekordów do tabeli 'firmy'")
    except Exception as e:
        print(f"Błąd 'firmy': {e}")


def miasta(con, sciezka_pliku, num_records=80):
    def wczytaj_miasta(sciezka_pliku):
            miasta = {}
            wojewodztwo = None
            with open(sciezka_pliku, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    if line.endswith(':'): 
                        wojewodztwo = line[:-1]
                        miasta[wojewodztwo] = []
                    elif wojewodztwo: 
                        miasta[wojewodztwo].append(line)
            return miasta  
    
    polskie_miasta = wczytaj_miasta(sciezka_pliku)
    try:
        kraje = con.execute(text("SELECT id_kraju, kraj FROM kraje")).fetchall()
        kraje_dict = {kraj.strip().lower(): id_kraju for id_kraju, kraj in kraje}
        polska_id = kraje_dict.get("polska")
        miasta_polskie = []
        for wojewodztwo, lista_miast in polskie_miasta.items():
            for miasto in lista_miast:
                miasta_polskie.append((miasto, wojewodztwo))

        random.shuffle(miasta_polskie)
        miasta_polskie = random.sample(miasta_polskie, min(len(miasta_polskie), int(num_records)))
        
        for miasto, województwo in miasta_polskie:
            query = text(""" 
                INSERT INTO miasta (miasto, id_kraju, województwo)
                VALUES (:miasto, :id_kraju, :województwo)
            """)
            con.execute(query, {"miasto": miasto, "id_kraju": polska_id, "województwo": województwo})

        print(f"Wstawiono {len(miasta_polskie)} rekordów do tabeli 'miasta'")
    except Exception as e:
        print(f"Błąd 'miasta': {e}")

def wycieczki(con, sciezka_nazwy_wycieczek, sciezka_rodzaje_wycieczek, liczba_rekordow=100):
    try:
        with open(sciezka_nazwy_wycieczek, "r", encoding="utf-8") as f:
            nazwy_wycieczek = [line.strip() for line in f if line.strip()]

        with open(sciezka_rodzaje_wycieczek, "r", encoding="utf-8") as f:
            rodzaje_wycieczek = [line.strip() for line in f if line.strip()]

        kraje = con.execute(text("SELECT id_kraju, id_waluty FROM kraje")).fetchall()
        kraje_lista = [(row[0], row[1]) for row in kraje]
        waluty = list(set([row[1] for row in kraje_lista]))

        przypisane_waluty = []
        status_w_ofercie = {}

        for _ in range(liczba_rekordow):
            nazwa = random.choice(nazwy_wycieczek)
            rodzaj = random.choice(rodzaje_wycieczek)

            if nazwa not in status_w_ofercie:
                status_w_ofercie[nazwa] = random.choice([0, 1])
            czy_w_aktualnej_ofercie = status_w_ofercie[nazwa]
            długość_wycieczki = random.randint(1, 30)

            if len(przypisane_waluty) < len(waluty):
                id_kraju_głównego, id_waluty = random.choice([row for row in kraje_lista if row[1] not in przypisane_waluty])
                przypisane_waluty.append(id_waluty)
            else:
                id_kraju_głównego, id_waluty = random.choice(kraje_lista)

            if rodzaj in ["Lokalne zwiedzanie", "Wycieczka szkolna", "Grupa seniorów","Grupa rodzinna"]:
                czy_ekstremalna = 0
            else:
                czy_ekstremalna = random.choice([0, 1])

            czy_zapewniony_nocleg = 1 if długość_wycieczki > 1 else 0
            czy_zapewnione_wyżywienie = random.choice([0, 1])
            długość_zapewnionego_transportu = random.randint(500, 2000)
            
            #cena_za_osobę (w zł)
            cena_za_osobę = 100 
            cena_za_osobę += długość_wycieczki * 50 
            if czy_ekstremalna:
                cena_za_osobę *= 1.5 
            if czy_zapewniony_nocleg:
                cena_za_osobę += długość_wycieczki * 50 
            if czy_zapewnione_wyżywienie:
                cena_za_osobę +=  długość_wycieczki * 30
            cena_za_osobę = round(cena_za_osobę, 2) 
            wartość_zniżki_grupowej = round(random.uniform(0.0, 0.5), 2) if random.choice([True, False]) else None

            query = text("""
                INSERT INTO wycieczki (
                    nazwa_wycieczki, 
                    czy_w_aktualnej_ofercie, 
                    rodzaj_wycieczki, 
                    długość_wycieczki, 
                    id_kraju_głównego, 
                    czy_ekstremalna, 
                    czy_zapewniony_nocleg, 
                    czy_zapewnione_wyżywienie, 
                    długość_zapewnionego_transportu, 
                    cena_za_osobę, 
                    id_waluty, 
                    wartość_zniżki_grupowej
                ) VALUES (
                    :nazwa, 
                    :czy_w_aktualnej_ofercie, 
                    :rodzaj, 
                    :długość_wycieczki, 
                    :id_kraju_głównego, 
                    :czy_ekstremalna, 
                    :czy_zapewniony_nocleg, 
                    :czy_zapewnione_wyżywienie, 
                    :długość_zapewnionego_transportu, 
                    :cena_za_osobę, 
                    :id_waluty, 
                    :wartość_zniżki_grupowej
                )
            """)
            con.execute(query, {
                "nazwa": nazwa,
                "czy_w_aktualnej_ofercie": czy_w_aktualnej_ofercie,
                "rodzaj": rodzaj,
                "długość_wycieczki": długość_wycieczki,
                "id_kraju_głównego": id_kraju_głównego,
                "czy_ekstremalna": czy_ekstremalna,
                "czy_zapewniony_nocleg": czy_zapewniony_nocleg,
                "czy_zapewnione_wyżywienie": czy_zapewnione_wyżywienie,
                "długość_zapewnionego_transportu": długość_zapewnionego_transportu,
                "cena_za_osobę": cena_za_osobę,
                "id_waluty": id_waluty,
                "wartość_zniżki_grupowej": wartość_zniżki_grupowej
            })

        print(f"Wstawiono {liczba_rekordow} rekordów do tabeli 'wycieczki'")
    except Exception as e:
        print(f"Błąd 'wycieczki': {e}")

def adresy(con, sciezka_aleja, sciezka_plac_skwer, sciezka_general_marszalek, num_records=140):

    def wczytaj_adresy(sciezka):
        with open(sciezka, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    aleja = wczytaj_adresy(sciezka_aleja)
    plac = wczytaj_adresy(sciezka_plac_skwer)
    general = wczytaj_adresy(sciezka_general_marszalek)

    try:
        miasta = con.execute(text("SELECT id_miasta FROM miasta")).fetchall()
        miasta_list = [row[0] for row in miasta]

        for _ in range(num_records):
            id_miasta = random.choice(miasta_list)
            typ_ulicy = random.choice(["Ul.", "Plac", "Skwer", "Generała", "Marszałka", "Aleja"])
            if typ_ulicy == "Aleja" and aleja:
                ulica = f"Aleja {random.choice(aleja)}"
            elif typ_ulicy in ["Plac", "Skwer"] and plac:
                ulica = f"{typ_ulicy.capitalize()} {random.choice(plac)}"
            elif typ_ulicy in ["Generała", "Marszałka"]:
                ulica = f"{typ_ulicy.capitalize()} {random.choice(general)}"
            elif typ_ulicy == "Ul.":
                ulica = f"{typ_ulicy.capitalize()} {faker_pl.street_name()}"

            ulica = ulica[:50]
            numer_domu = faker_pl.building_number()[:10]
            numer_mieszkania = faker_pl.building_number()[:10] if random.choice([True, False]) else None

            query = text("""
                INSERT INTO adresy (id_miasta, ulica, numer_domu, numer_mieszkania)
                VALUES (:id_miasta, :ulica, :numer_domu, :numer_mieszkania)
            """)
            con.execute(query, {
                "id_miasta": id_miasta,
                "ulica": ulica,
                "numer_domu": numer_domu,
                "numer_mieszkania": numer_mieszkania
            })

        print(f"Wstawiono {num_records} adresów do tabeli 'adresy'")
    
    except Exception as e:
        print(f"Błąd 'adresy': {e}")

def koszt(con, num_records=80):
    try:
        kraje = con.execute(text("SELECT id_kraju, kraj, id_waluty FROM kraje WHERE kraj != 'Polska'")).fetchall()
        waluty = con.execute(text("SELECT id_waluty, kurs_na_zł FROM waluty")).fetchall()
        firmy = con.execute(text("SELECT id_firmy, id_kraju FROM firmy")).fetchall()
        kursy_walut = {row[0]: row[1] for row in waluty}
        waluty_list = [row[0] for row in kraje]
        kraje_waluta = {row[0]: row[2] for row in kraje}
        firmy_map = {}
        for firma in firmy:
            if firma[1] not in firmy_map:
                firmy_map[firma[1]] = []
            firmy_map[firma[1]].append(firma[0]) 
        
        for _ in range(num_records):
            id_kraju = random.choice(waluty_list)  
            id_waluty = kraje_waluta[id_kraju] 

            kurs_waluty = kursy_walut.get(id_waluty, 1)
            if id_waluty == 12:
                kurs_waluty = 1 

            rodzaje_kosztow = [
                "nocleg", "autobus", "bilety lotnicze",
                "wyżywienie", "przewodnik", "ubezpieczenie", "transport lokalny"]
            num_koszt = 3
            losowe_koszty = random.sample(rodzaje_kosztow, num_koszt)
            
            for rodzaj_kosztu in losowe_koszty:
                nazwa = f"{rodzaj_kosztu}"[:50]  

                if rodzaj_kosztu == "bilety lotnicze":
                    wartość = round(random.uniform(500, 3000), 2)
                elif rodzaj_kosztu in ["nocleg"]:
                    wartość = round(random.uniform(1500, 2500), 2)
                elif rodzaj_kosztu in ["autobus", "transport lokalny"]:
                    wartość = round(random.uniform(50, 300), 2)
                elif rodzaj_kosztu in ["wyżywienie"]:
                    wartość = round(random.uniform(300, 500), 2)
                elif rodzaj_kosztu in ["przewodnik", "ubezpieczenie"]:
                    wartość = round(random.uniform(500, 1000), 2)
                else:
                    wartość = round(random.uniform(50, 2000), 2)

                czy_zależne_od_osób = random.choice([0, 1, None])
                czy_zależne_od_dystansu = random.choice([0, 1, None]) if czy_zależne_od_osób is None else None
                czy_zależne_od_liczby_dni = random.choice([0, 1])
                czy_zależne_od_liczby_osób = random.choice([0,1])
                if czy_zależne_od_osób is None and czy_zależne_od_dystansu is None:
                    czy_zależne_od_liczby_dni = 1

                if czy_zależne_od_osób:
                    wartość *= 1.2 
                if czy_zależne_od_dystansu:
                    wartość *= 1.15 
                if czy_zależne_od_liczby_dni:
                    wartość *= 1.1 
                if id_waluty != 12: 
                    if kurs_waluty > 1:
                        wartość = round(wartość / kurs_waluty, 2)  
                    else:
                        wartość = round(wartość * kurs_waluty, 2)  

                id_firmy = None
                if id_kraju in firmy_map:  
                    id_firmy = random.choice(firmy_map[id_kraju])

                query = text(""" 
                    INSERT INTO koszt (nazwa, id_waluty, wartość, id_firmy, 
                                        czy_zależne_od_liczby_osób, czy_zależne_od_dystansu, czy_zależne_od_liczby_dni)
                    VALUES (:nazwa, :id_waluty, :wartość, :id_firmy, :czy_zależne_od_liczby_osób, 
                            :czy_zależne_od_dystansu, :czy_zależne_od_liczby_dni)
                """)
                con.execute(query,{
                    'nazwa': nazwa,
                    'id_waluty': id_waluty,
                    'wartość': wartość,
                    'id_firmy': id_firmy,
                    'czy_zależne_od_liczby_osób': czy_zależne_od_liczby_osób,
                    'czy_zależne_od_dystansu': czy_zależne_od_dystansu,
                    'czy_zależne_od_liczby_dni': czy_zależne_od_liczby_dni
                })
        print(f"Wstawiono {num_records} rekordów do tabeli 'koszty'")
    except Exception as e:
        print(f"Błąd 'koszt': {e}")

def pracownicy(con, sciezka_pliku, num_records=40):
    try:
        stanowiska = con.execute(text("SELECT id_stanowiska, stanowisko FROM stanowiska")).fetchall()
        adresy = con.execute(text("SELECT id_adresu, id_miasta FROM adresy")).fetchall()
        dostepne_adresy = [row[0] for row in adresy]
        stanowiska_map = {row[1]: row[0] for row in stanowiska}

        imiona_meskie = []
        imiona_zenskie = []
        with open(sciezka_pliku, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                imie, plec, waga = row[0], row[1], int(row[2])
                if plec == 'M':
                    imiona_meskie.extend([imie] * waga)
                elif plec == 'K':
                    imiona_zenskie.extend([imie] * waga)

        def wylosuj_imie(imiona):
            return random.choice(imiona)
    
        dokumenty_uzyte = set()
        emaile_uzyte = set()
        telefony_uzyte = set()

        srednia_wiek = 35
        odchylenie_wiek = 10
        minimalny_wiek_na_prace = 16

        num_przewodnikow = random.randint(6,8)
        num_pro_przewodnik = random.randint(1,4)
        num_agentow = random.randint(7,9)
        num_dyrektorow = random.randint(1,2)
        num_innych_pracownikow = num_records - (num_przewodnikow + num_pro_przewodnik + num_agentow + num_dyrektorow )
        stanowiska_list = ['Przewodnik'] * num_przewodnikow + ['Przewodnik Pro'] * num_pro_przewodnik + ['Agent'] * num_agentow + ['Dyrektor'] * num_dyrektorow

        if num_innych_pracownikow > 0:
            wszystkie_stanowiska = [row[1] for row in stanowiska if row[1] not in ['Przewodnik', 'Przewodnik Pro', 'Agent', 'Dyrektor']]
            losowe_stanowiska = random.choices(wszystkie_stanowiska, k=num_innych_pracownikow)
            stanowiska_list.extend(losowe_stanowiska)
        adresy_pracownicy = random.sample(dostepne_adresy, num_records)
        adresy_uzyte = iter(adresy_pracownicy)

        for stanowisko in stanowiska_list:
            id_stanowiska = stanowiska_map.get(stanowisko)  
            id_adresu = next(adresy_uzyte, None)

            if random.choice([True, False]):
                imię = wylosuj_imie(imiona_meskie)[:20]
            else:
                imię = wylosuj_imie(imiona_zenskie)[:20]
            nazwisko = faker_pl.last_name()[:50]

            while True:
                wiek = int(random.gauss(srednia_wiek, odchylenie_wiek))
                if wiek >= minimalny_wiek_na_prace:
                    break
            data_urodzenia = datetime.now() - timedelta(days=wiek * 365.25)

            while True:
                nr_dokumentu = faker_pl.unique.bothify(text='??########')[:30]
                if nr_dokumentu not in dokumenty_uzyte:
                    dokumenty_uzyte.add(nr_dokumentu)
                    break

            while True:
                domena = faker_pl.domain_name()
                email = f"{imię.lower()}.{nazwisko.lower()}@{domena}"[:50]
                if email not in emaile_uzyte:
                    emaile_uzyte.add(email)
                    break
            while True:
                numer_telefonu = f"+48 {Faker('pl_PL').msisdn()[:9]}"
                if numer_telefonu not in telefony_uzyte:
                    telefony_uzyte.add(numer_telefonu)
                    break

            min_data_rozpoczecia = data_urodzenia + timedelta(days=minimalny_wiek_na_prace * 365)  

            if id_stanowiska in [1, 2]:  
                min_data = max(min_data_rozpoczecia, datetime.now() - timedelta(days=6*365))  
                max_data = datetime.now() - timedelta(days=5 * 365)  
            else:  
                min_data = max(min_data_rozpoczecia, datetime.now() - timedelta(days=5*365))  
                max_data = datetime.now()  

            roznica_dni = (max_data - min_data).days  

            if roznica_dni > 0:
                losowa_liczba_dni = random.randint(0, roznica_dni)
                data_rozpoczęcia_pracy = min_data + timedelta(days=losowa_liczba_dni)
            else:
                data_rozpoczęcia_pracy = min_data

            query = text("""
                        INSERT INTO pracownicy (
                            id_stanowiska, id_adresu, imię, nazwisko, 
                            nr_dokumentu, email, numer_telefonu, data_urodzenia, data_rozpoczęcia_pracy
                        )
                        VALUES (
                            :id_stanowiska, :id_adresu, :imię, :nazwisko, 
                            :nr_dokumentu, :email, :numer_telefonu, :data_urodzenia, :data_rozpoczęcia_pracy
                        )
                    """)
            con.execute(query, {
                        "id_stanowiska": id_stanowiska,
                        "id_adresu": id_adresu,
                        "imię": imię,
                        "nazwisko": nazwisko,
                        "nr_dokumentu": nr_dokumentu,
                        "email": email,
                        "numer_telefonu": numer_telefonu,
                        "data_urodzenia": data_urodzenia,
                        "data_rozpoczęcia_pracy": data_rozpoczęcia_pracy
                    })

        print(f"Wstawiono {num_records} rekordów do tabeli 'pracownicy'")

    except Exception as e:
        print(f"Błąd 'pracownicy': {e}")

def klienci(con, sciezka_pliku, num_records=100):
    try:
        adresy = con.execute(text("SELECT id_adresu FROM adresy")).fetchall()
        polskie_adresy = [row[0] for row in adresy]
        imiona_meskie = []
        imiona_zenskie = []

        with open(sciezka_pliku, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                imie, plec, waga = row[0], row[1], int(row[2])
                if plec == 'M':
                    imiona_meskie.append((imie, waga))
                elif plec == 'K':
                    imiona_zenskie.append((imie, waga))

        def wylosuj_imie(imiona):
            imiona_lista = [imie for imie, waga in imiona for _ in range(waga)]
            return random.choice(imiona_lista)
        dokumenty_uzyte = set()
        telefony_uzyte = set()

        srednia_wiek = 35  
        odchylenie_wiek = 10  

        for _ in range(num_records):
            adresy_klienci = random.sample(polskie_adresy, min(len(polskie_adresy), num_records))
            id_adresu = adresy_klienci.pop()

            if random.choice([True, False]): 
                imię = wylosuj_imie(imiona_meskie)[:20]
            else:
                imię = wylosuj_imie(imiona_zenskie)[:20]
            nazwisko = faker_pl.last_name()[:50]
            imię_kontaktowe = imię
            nazwisko_kontaktowe = nazwisko
            while True:
                wiek = int(random.gauss(srednia_wiek, odchylenie_wiek))
                if 5 <= wiek <= 90:
                    break

            if wiek < 13:
                numer_telefonu = random.choice([None, f"+48 {faker_pl.msisdn()[:9]}"])
                imię_kontaktowe = random.choice([None, imię])
                nazwisko_kontaktowe = nazwisko if imię_kontaktowe == imię else None
            else:
                while True:
                    numer_telefonu = f"+48 {faker_pl.msisdn()[:9]}"
                    if numer_telefonu not in telefony_uzyte:
                        telefony_uzyte.add(numer_telefonu)
                        break
                imię_kontaktowe = imię
                nazwisko_kontaktowe = nazwisko
                nr_telefonu_kontaktowy = numer_telefonu

            data_urodzenia = datetime.now() - timedelta(days=wiek * 365.25)
            while True:
                nr_dokumentu = faker_pl.unique.bothify(text='??########')[:30]
                if nr_dokumentu not in dokumenty_uzyte:
                    dokumenty_uzyte.add(nr_dokumentu)
                    break

            query = text("""
                INSERT INTO klienci (
                    imię, nazwisko, nr_telefonu, data_urodzenia,
                    nr_dokumentu, id_adresu,
                    imię_kontaktowe, nazwisko_kontaktowe, nr_telefonu_kontaktowy
                )
                VALUES (
                    :imię, :nazwisko, :nr_telefonu, :data_urodzenia,
                    :nr_dokumentu, :id_adresu,
                    :imię_kontaktowe, :nazwisko_kontaktowe, :nr_telefonu_kontaktowy
                )
            """)
            con.execute(query, {
                "imię": imię,
                "nazwisko": nazwisko,
                "nr_telefonu": numer_telefonu,
                "data_urodzenia": data_urodzenia.date(),
                "nr_dokumentu": nr_dokumentu,
                "id_adresu": id_adresu,
                "imię_kontaktowe": imię_kontaktowe,
                "nazwisko_kontaktowe": nazwisko_kontaktowe,
                "nr_telefonu_kontaktowy": numer_telefonu
            })

        print(f"Wstawiono {num_records} rekordów do tabeli 'klienci'")
    except Exception as e:
        print(f"Błąd 'klienci': {e}")

def transakcje(con, num_records=100):
    try:
        klienci = con.execute(text("SELECT id_klienta, data_urodzenia FROM klienci")).fetchall()
        def oblicz_wiek(data_urodzenia):
            return date.today().year - data_urodzenia.year - ((date.today().month, date.today().day) < (data_urodzenia.month, data_urodzenia.day))

        def transakcja(wiek):
            opcje = ["przelew", "gotówka", "bon"]
        
            if wiek <= 26:
                wagi = [0.8, 0.15, 0.05] 
            elif wiek >= 40:
                wagi = [0.3, 0.6, 0.1] 
            else:
                wagi = [0.45, 0.45, 0.1] 
            return random.choices(opcje, weights=wagi, k=1)[0]

        klienci_list = [{"id": row[0], "wiek": oblicz_wiek(row[1])} for row in klienci]

        for _ in range(num_records):
            klient = random.choice(klienci_list)
            wiek = klient["wiek"]
            id_płatnika = klient["id"]
            typ_transakcji = transakcja(wiek)
            numer_konta_płatnika = faker_pl.iban() if typ_transakcji == "przelew" else None
            data_transakcji = faker_pl.date_between(start_date="-5y", end_date="today")
            data_zaksięgowania = data_transakcji + timedelta(days=random.randint(1, 5))

            query = text("""
                INSERT INTO transakcje (
                   typ_transakcji, numer_konta_płatnika, id_płatnika, data_transakcji, data_zaksięgowania
                )
                VALUES (
                 :typ_transakcji, :numer_konta_płatnika, :id_płatnika, :data_transakcji, :data_zaksięgowania
                )
            """)

            con.execute(query, {
                "typ_transakcji": typ_transakcji,
                "numer_konta_płatnika": numer_konta_płatnika,
                "id_płatnika": id_płatnika,
                "data_transakcji": data_transakcji,
                "data_zaksięgowania": data_zaksięgowania
            })

        print(f"Wstawiono {num_records} rekordów do tabeli 'transakcje'")
    
    except Exception as e:
        print(f"Błąd 'transakcje': {e}")

def wyjazdy(con):
    try:
        wycieczki = con.execute(text("SELECT id_wycieczki, długość_wycieczki FROM wycieczki")).fetchall()
        pracownicy = con.execute(text("SELECT id_pracownika, id_stanowiska, data_rozpoczęcia_pracy FROM pracownicy")).fetchall()
        adresy = con.execute(text("SELECT id_adresu FROM adresy")).fetchall()
        adresy_list = [row[0] for row in adresy]  
        agenci_list = [row[0] for row in pracownicy if row[1] == 13] 
        przewodnicy_dict = {row[0]: row[2] for row in pracownicy if row[1] in (1, 2)}

        for id_wycieczki, długość_wycieczki in wycieczki:
            id_adresu_zbiórki = random.choice(adresy_list)

            data_rozpoczęcia = faker_default.date_between(start_date="-5y", end_date="today")
            data_zakończenia = data_rozpoczęcia + timedelta(days=długość_wycieczki)
            dostępni_przewodnicy = [id_p for id_p, data_start in przewodnicy_dict.items() if data_start <= data_rozpoczęcia]
            id_przewodnika = random.choice(dostępni_przewodnicy)
            id_agenta = random.choice(agenci_list)

            query = text("""
                INSERT INTO wyjazdy (
                    id_przewodnika, id_agenta, id_wycieczki,
                    data_rozpoczęcia, data_zakończenia, id_adresu_zbiórki
                )
                VALUES (
                    :id_przewodnika, :id_agenta, :id_wycieczki,
                    :data_rozpoczęcia, :data_zakończenia, :id_adresu_zbiórki
                )
            """)
            con.execute(query, {
                "id_przewodnika": id_przewodnika,
                "id_agenta": id_agenta,
                "id_wycieczki": id_wycieczki,
                "data_rozpoczęcia": data_rozpoczęcia,
                "data_zakończenia": data_zakończenia,
                "id_adresu_zbiórki": id_adresu_zbiórki
            })

        print(f"Wstawiono dane 'wyjazdy'")
    except Exception as e:
        print(f"Błąd 'wyjazdy': {e}")

def grupa_klient(con):
    try:
        grupy = con.execute(text("SELECT id_grupy, typ_grupy FROM grupy")).fetchall()
        klienci = con.execute(text("SELECT id_klienta FROM klienci")).fetchall()
        grupy_list = [row[0] for row in grupy]
        rodzaje_grup = {row[0]: row[1] for row in grupy} 
        klienci_list = [row[0] for row in klienci]
        random.shuffle(klienci_list)
        total_klienci = len(klienci_list)

        klienci_niedost = set()
        for i, id_grupy in enumerate(grupy_list):
            rodzaj_grupy = rodzaje_grup[id_grupy]

            if rodzaj_grupy == "Jednoosobowa grupa":
                num_members = 1
            elif rodzaj_grupy == "Romantyczny wyjazd we dwoje":
                num_members = 2
            elif rodzaj_grupy == "Wycieczka szkolna":
                num_members = random.randint(20, 25)
            elif rodzaj_grupy == "Grupa rodzinna":
                num_members = random.randint(3,6)
            elif rodzaj_grupy == "Grupa seniorów":
                num_members = random.randint(10,20)
            else:
                num_members = random.randint(2, min(10, total_klienci))

            klienci_dostepni = [c for c in klienci_list if c not in klienci_niedost]
            if len(klienci_dostepni) < num_members:
                members = random.sample(klienci_list, num_members)
            else:
                members = random.sample(klienci_dostepni, num_members)

            klienci_niedost.update(members)
            id_koordynatora = random.choice(members)

            for id_klienta in members:
                czy_koordynator_grupy = (id_klienta == id_koordynatora)

                query = text("""
                    INSERT INTO grupa_klient (
                        id_grupy, id_klienta, czy_koordynator_grupy
                    )
                    VALUES (
                        :id_grupy, :id_klienta, :czy_koordynator_grupy
                    )
                """)
                con.execute(query, {
                    "id_grupy": id_grupy,
                    "id_klienta": id_klienta,
                    "czy_koordynator_grupy": czy_koordynator_grupy
                })

        print(f"Wstawiono dane 'grupa_klient'")
    except Exception as e:
        print(f"Błąd 'grupa_klient': {e}")

def grupa_wyjazd(con):
    try:
        grupy = con.execute(text("SELECT id_grupy FROM grupy")).fetchall()
        wyjazdy = con.execute(text("SELECT id_wyjazdu FROM wyjazdy")).fetchall()
        transakcje = con.execute(text("SELECT id_transakcji FROM transakcje")).fetchall()
        grupy_list = [row[0] for row in grupy]
        wyjazdy_list = [row[0] for row in wyjazdy]
        transakcje_list = [row[0] for row in transakcje]
        grupy_przypisane = set()  
        wyjazdy_przypisane = set()  

        for id_grupy in grupy_list:
            id_wyjazdu = random.choice(wyjazdy_list)  
            id_transakcji = random.choice(transakcje_list)  

            query = text("""
                INSERT INTO grupa_wyjazd (
                    id_grupy, id_wyjazdu, id_transakcji
                )
                VALUES (
                    :id_grupy, :id_wyjazdu, :id_transakcji
                )
            """)
            con.execute(query, {
                "id_grupy": id_grupy,
                "id_wyjazdu": id_wyjazdu,
                "id_transakcji": id_transakcji
            })

            grupy_przypisane.add(id_grupy)
            wyjazdy_przypisane.add(id_wyjazdu)

        for id_wyjazdu in wyjazdy_list:
            if id_wyjazdu not in wyjazdy_przypisane:
                id_grupy = random.choice(grupy_list)
                id_transakcji = random.choice(transakcje_list) 

                query = text("""
                    INSERT INTO grupa_wyjazd (
                        id_grupy, id_wyjazdu, id_transakcji
                    )
                    VALUES (
                        :id_grupy, :id_wyjazdu, :id_transakcji
                    )
                """)
                con.execute(query, {
                    "id_grupy": id_grupy,
                    "id_wyjazdu": id_wyjazdu,
                    "id_transakcji": id_transakcji
                })

                grupy_przypisane.add(id_grupy)
                wyjazdy_przypisane.add(id_wyjazdu)

        print(f"Wstawiono dane 'grupa_wyjazdy'")

    except Exception as e:
        print(f"Błąd 'grupa_wyjazd': {e}")

def koszt_wyjazd(con):
    try:
        koszt = con.execute(text("""SELECT id_kosztu, id_waluty FROM koszt""")).fetchall()
        wyjazdy = con.execute(text("""SELECT id_wyjazdu, id_wycieczki FROM wyjazdy""")).fetchall()
        wycieczki = con.execute(text("""SELECT id_wycieczki, id_waluty, id_kraju_głównego FROM wycieczki""")).fetchall()
        wycieczki_map = {row[0]: (row[1], row[2]) for row in wycieczki} 

        for id_kosztu, id_waluty in koszt:
            pasujace_wycieczki = [
                (id_wycieczki, id_waluty_wycieczki, id_kraju_głównego)
                for id_wycieczki, (id_waluty_wycieczki, id_kraju_głównego) in wycieczki_map.items()
                if id_waluty_wycieczki == id_waluty]
            
            if not pasujace_wycieczki:
                continue

            wycieczka = random.choice(pasujace_wycieczki)
            id_wycieczki, _, _ = wycieczka  
            pasujace_wyjazdy = [id_wyjazdu for id_wyjazdu, id_wycieczka in wyjazdy if id_wycieczka == id_wycieczki]
            
            if not pasujace_wyjazdy:
                continue

            id_wyjazdu = random.choice(pasujace_wyjazdy)

            query = text("""
                INSERT INTO koszt_wyjazd (
                    id_kosztu, id_wyjazdu
                )
                VALUES (
                    :id_kosztu, :id_wyjazdu
                )
            """)
            con.execute(query, {
                "id_kosztu": id_kosztu,
                "id_wyjazdu": id_wyjazdu
            })
        print(f"Wstawiono dane 'koszt_wyjazdu'")

    except Exception as e:
        print(f"Błąd 'koszt_wyjazd': {e}")
        
if __name__ == "__main__":
        waluty(con, "kraje_waluty.csv")
        kraje(con, "kraje_waluty.csv")
        grupy(con)
        stanowiska(con)
        firmy(con)
        miasta(con, "miasta.txt")
        wycieczki(con, "nazwa_wycieczki.txt", "rodzaj_wycieczki.txt")
        adresy(con, "aleja.txt", "plac_skwer.txt", "generał_marszałek.txt")
        koszt(con)
        pracownicy(con, "imiona.csv")
        klienci(con, "imiona.csv")
        transakcje(con)
        wyjazdy(con)
        grupa_klient(con)
        grupa_wyjazd(con)
        koszt_wyjazd(con)
        con.commit()  
        print("Dane zostały zapisane")
        con.close()