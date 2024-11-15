import json
import random
import asyncio
import re
from datetime import datetime, timedelta
from functools import wraps
import httpx
from httpx import AsyncClient
import pandas as pd

"""
- Skript stahuje ze stránky booking.com kapacity a ceny všech ubytování v Brně.
- Cenou je myšlena NEJNIŽŠÍ cena pro osobu a noc, jakou má hotel na daný den k dispozici.
- Skript ke svému fungování potřebuje seznam hotelů, uložený ve formátu json ve stejném adresáři. Tento seznam byl stažen
pomocí aplikace https://apify.com/voyager/booking-scraper (freemium) a dá se tam stáhnout kdykoliv znovu.
- autor nápadu: https://scrapfly.io/blog/how-to-scrape-bookingcom/
- skript na GitHubu: https://github.com/krspack
"""


# Vstupy od uživatele
PRIJEZD = "2024-12-02"  # YYYY-MM-DD
POKOJU = 1
NOCI = 2
DOSPELYCH = 1  # pro zjednodušení se berou v uvahu jen dospeli
VYSLEDEK_ULOZIT_DO = ["hotely_jmk.csv", "pokoje_jmk.csv", "pro_mapu_kapacit_jmk.csv"]

SLEEP_LIMIT = 150  # pauza mezi requesty
MAX_RETRIES = 10  # maximum pokusů
RETRY_DELAY = 50  # pauza mezi pokusy

# Načtení seznamu brněnských hotelů, staženého pomocí https://apify.com/voyager/booking-scraper
with open("dataset_booking-scraper_2024-09-19_12-33-06-898.json") as file:
    text = file.read()
    text_dict = json.loads(text)

# Vyber relevantní informace, ulož do tabulky all_hotels
def vyber(seznam_hotelu, prijezd, pocet_noci):
    hotels_temporary = []
    for d in seznam_hotelu:
        try:
            hotel_dict = {
                "poradi": d["order"],
                "url": d["url"].split("?")[0],
                "jmeno": d["name"],
                "typ": d["type"],
                "lat": list(d["location"].values())[0],
                "lng": list(d["location"].values())[1],
                "adresa_ulice": d["address"]["street"],
                "adresa_cela": d["address"]["full"],
                "pokoje": d["rooms"],
            }
        except AttributeError:
            print('nekompletni udaje ohledne:', d['url'])
            continue
        hotels_temporary.append(hotel_dict)
    all_hotels_df = pd.DataFrame(hotels_temporary, columns=hotels_temporary[0].keys())
    all_hotels_df['kapacita'] = None

    if not isinstance(pocet_noci, int) or pocet_noci <= 0:
        raise ValueError("Počet nocí musí být kladné celé číslo.")
    for _ in range(pocet_noci):
        date = datetime.strptime(prijezd, "%Y-%m-%d") + timedelta(days = _)
        date = datetime.strftime(date, "%Y-%m-%d")
        all_hotels_df[date + "_min_cena"] = None
        all_hotels_df[date + "_min_noci"] = None
    return all_hotels_df


all_hotels = vyber(text_dict, PRIJEZD, NOCI)


# Zjisti kapacitu hotelů, vytvoř tabulku all_rooms
def zjisti_kapacitu(hotels):
    rooms_list = []
    for index, hotel in hotels.iterrows():
        hotel_order = hotel["poradi"]
        hotel_name = hotel["jmeno"]
        hotel_capacity = 0
        for room in hotel["pokoje"]:
            room_details = {
                "hotel_poradi": hotel_order,
                "hotel_jmeno": hotel_name,
                "id_pokoje": room.get("id"),
                "url": room.get("url"),
                "typ": room.get("roomType"),
                "kapacita": room.get("persons"),
            }
            hotel_capacity += room.get("persons")
            rooms_list.append(room_details)
        hotels.at[index, "pokoje"] = [
            {"id": room["id"], "persons": room["persons"]} for room in hotel["pokoje"]
        ]
        hotels.at[index, "kapacita"] = hotel_capacity
    rooms = pd.DataFrame(rooms_list)
    return rooms


all_rooms = zjisti_kapacitu(all_hotels)


# Kontrola vstupů od uživatele pro pozdější funkci scrap_hotel
def validate_inputs(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        url = kwargs.get("url")
        start_date = kwargs.get("start_date")
        days = kwargs.get("days")
        adults = kwargs.get("adults")
        rooms = kwargs.get("rooms")

        # Validating input types
        if not isinstance(url, str):
            raise ValueError("URL musí být string.")
        if not isinstance(start_date, str) or not re.match(
            r"\d{4}-\d{2}-\d{2}", start_date
        ):
            raise ValueError("Formát pro datum: YYYY-MM-DD.")
        if not isinstance(days, int) or days <= 0:
            raise ValueError("Počet nocí musí být kladné celé číslo.")
        if not isinstance(adults, int) or adults <= 0:
            raise ValueError("Počet hostů musí být kladné celé číslo.")
        if not isinstance(rooms, int) or rooms <= 0:
            raise ValueError("Počet pokojů musí být kladné celé číslo.")

        return await func(*args, **kwargs)

    return wrapper


# U hotelů, co mají volné pokoje pro zadané datum, zjistit cenu za pokoj. Jedná se o nejlevnější právě volné místo.
@validate_inputs
async def scrape_hotel(
    url: str,
    session: AsyncClient,
    start_date: str,
    days: int = 1,
    adults: int = 1,
    rooms: int = 1,
):
    for attempt in range(MAX_RETRIES):

        try:
            await asyncio.sleep(random.uniform(0, SLEEP_LIMIT))
            resp = await session.get(url, timeout=10.0)
            html_content = resp.text

            # Ze stránky hotelu vytáhnout údaje pro pozdější GraphQL dotaz
            hotel_country_match = re.search(r'hotelCountry:\s*"(.+?)"', html_content)
            hotel_name_match = re.search(r'hotelName:\s*"(.+?)"', html_content)
            csrf_token_match = re.search(r"b_csrf_token:\s*'(.+?)'", html_content)

            if not (hotel_country_match and hotel_name_match and csrf_token_match):
                print(f"Nenelazeno: {url}")
                return None

            hotel_country = hotel_country_match.group(1)
            hotel_name = hotel_name_match.group(1)
            csrf_token = csrf_token_match.group(1)

            # GraphQL dotaz se zjištěným tokenem (csrf_token)
            gql_body = json.dumps(
                {
                    "operationName": "AvailabilityCalendar",
                    "variables": {
                        "input": {
                            "travelPurpose": 2,
                            "pagenameDetails": {
                                "countryCode": hotel_country,
                                "pagename": hotel_name,
                            },
                            "searchConfig": {
                                "searchConfigDate": {
                                    "startDate": start_date,
                                    "amountOfDays": days,
                                },
                                "nbAdults": adults,
                                "nbRooms": rooms,
                            },
                        }
                    },
                    "query": "query AvailabilityCalendar($input: AvailabilityCalendarQueryInput!) {\n  availabilityCalendar(input: $input) {\n    ... on AvailabilityCalendarQueryResult {\n      hotelId\n      days {\n        available\n        avgPriceFormatted\n        checkin\n        minLengthOfStay\n        __typename\n      }\n      __typename\n    }\n    ... on AvailabilityCalendarQueryError {\n      message\n      __typename\n    }\n    __typename\n  }\n}\n",
                },
                separators=(",", ":"),
            )

            # Poslat GraphQL dotaz
            price_response = await session.post(
                "https://www.booking.com/dml/graphql?lang=en-gb",
                data=gql_body,
                headers={
                    "content-type": "application/json",
                    "x-booking-csrf-token": csrf_token,
                    "origin": "https://www.booking.com",
                },
            )
            price_data = price_response.json()["data"]["availabilityCalendar"]["days"]

            # Zjistit ceny z "price details"
            requested_days_prices = []
            for day in price_data:
                day_details = {
                    "date": day["checkin"],
                    "available": day["available"],
                    "price": day["avgPriceFormatted"],
                    "minLengthOfStay": day["minLengthOfStay"],
                }
                requested_days_prices.append(day_details)
            return {
                "hotel_name": hotel_name,
                "requested_days_prices": requested_days_prices,
            }

        except (httpx.ConnectTimeout, httpx.RequestError) as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
            else:
                return None
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} for {url}")
            return None


async def run_scrape(
    hotels: pd.DataFrame,
    start_date: str,
    days: int = 1,
    adults: int = 1,
    rooms: int = 1,
):
    async with AsyncClient(headers=HEADERS) as session:
        tasks = []
        indices = []

        for index, hotel in hotels.iterrows():
            # Kontrola, že hotel má požadovaný počet míst:
            if adults > hotel.get("kapacita", 0):
                print(
                    f"Požadovaný počet míst překračuje kapacitu ubytování {hotel['jmeno']}."
                )
                continue

            # Vytvoří task pro jeden hotel, zapamatuje si index pro spárování výsledků
            task = scrape_hotel(
                url=hotel["url"],
                session=session,
                start_date=start_date,
                days=days,
                adults=adults,
                rooms=rooms,
            )
            tasks.append(task)
            indices.append(index)

        # Posbírat výsledky scrapování stránek všech hotelů
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Projít výsledky a vepsat je do tabulky hotelů (sloupce cena, minimiální počet nocí)
        for idx, result in zip(indices, results):
            if isinstance(result, dict):
                for day in result["requested_days_prices"]:
                    date = day["date"]
                    if day["available"]:
                        price_formatted = int(
                            day["price"].replace("K", "00").replace(".", "")
                        )
                        hotels.loc[idx, date + "_min_cena"] = price_formatted
                        hotels.loc[idx, date + "_min_noci"] = day["minLengthOfStay"]
                    else:
                        hotels.loc[idx, date + "_min_cena"] = "obsazené"
                        hotels.loc[idx, date + "_min_noci"] = "-"
            elif isinstance(result, Exception):
                print(f"Chyba: {idx}, {result}")
    return


HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Accept-Language": "en-US,en;q=0.9",
}


if __name__ == "__main__":
    asyncio.run(
        run_scrape(
            hotels=all_hotels,
            start_date=PRIJEZD,
            days=NOCI,
            adults=DOSPELYCH,
            rooms=POKOJU,
        )
    )
    all_hotels.to_csv(VYSLEDEK_ULOZIT_DO[0], sep="\t", encoding="UTF-8")
    all_rooms.to_csv(VYSLEDEK_ULOZIT_DO[1], sep="\t", encoding="UTF-8")

def vyber_data_pro_mapu(hotels):
    mini_df = hotels[['jmeno', 'adresa_cela', 'lat', 'lng', 'kapacita']].copy()
    mini_df = mini_df.reset_index(drop=False, names ="hotel_index")
    mini_df.to_csv(VYSLEDEK_ULOZIT_DO[2], sep="\t", encoding="UTF-8")
    return
vyber_data_pro_mapu(all_hotels)







