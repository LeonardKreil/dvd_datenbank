-- Verbinden mit der dvdrental-Datenbank
\c dvdrental;

-- a. Gesamtanzahl der verfügbaren Filme
SELECT COUNT(*) AS total_movies FROM film;

-- b. Anzahl der unterschiedlichen Filme je Standort
SELECT store_id, COUNT(DISTINCT film_id) AS films_per_store
FROM inventory
GROUP BY store_id;

-- c. Die Vor- und Nachnamen der 10 Schauspieler mit den meisten Filmen, absteigend sortiert
SELECT actor.first_name, actor.last_name, COUNT(film_actor.film_id) AS film_count
FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
GROUP BY actor.actor_id
ORDER BY film_count DESC
LIMIT 10;

-- d. Erlöse je Mitarbeiter
SELECT staff.first_name, staff.last_name, SUM(payment.amount) AS revenue
FROM payment
JOIN staff ON payment.staff_id = staff.staff_id
GROUP BY staff.staff_id;

-- e. Die IDs der 10 Kunden mit den meisten Entleihungen
SELECT customer_id, COUNT(rental_id) AS rental_count
FROM rental
GROUP BY customer_id
ORDER BY rental_count DESC
LIMIT 10;

-- f. Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, die das meiste Geld ausgegeben haben
SELECT customer.first_name, customer.last_name, store.store_id, SUM(payment.amount) AS total_spent
FROM payment
JOIN customer ON payment.customer_id = customer.customer_id
JOIN rental ON rental.customer_id = customer.customer_id
JOIN inventory ON rental.inventory_id = inventory.inventory_id
JOIN store ON inventory.store_id = store.store_id
GROUP BY customer.customer_id, store.store_id
ORDER BY total_spent DESC
LIMIT 10;

-- g. Die 10 meistgesehenen Filme unter Angabe des Titels, absteigend sortiert
SELECT film.title, COUNT(rental.rental_id) AS view_count
FROM film
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
GROUP BY film.film_id
ORDER BY view_count DESC
LIMIT 10;

-- h. Die 3 meistgesehenen Filmkategorien
SELECT category.name AS category, COUNT(rental.rental_id) AS view_count
FROM category
JOIN film_category ON category.category_id = film_category.category_id
JOIN film ON film_category.film_id = film.film_id
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
GROUP BY category.category_id
ORDER BY view_count DESC
LIMIT 3;

-- j. Anzeigen der Passwörter der Mitarbeiter
SELECT staff_id, first_name, last_name, password
FROM staff;

-- k. Gesamtanzahl an Filmen je Standort
SELECT inventory.store_id, COUNT(inventory.film_id) AS total_films
FROM inventory
JOIN film ON inventory.film_id = film.film_id
GROUP BY inventory.store_id;

-- l. Anzahl an Filmen, die weniger als 60 Minuten dauern
SELECT COUNT(*) AS short_films
FROM film
WHERE length < 60;

-- m Anazahl an Entleihungen von Kurzfilmen
SELECT COUNT(*) AS total_short_film_rentals
FROM rental
JOIN inventory ON rental.inventory_id = inventory.inventory_id
JOIN film ON inventory.film_id = film.film_id
WHERE film.length < 60;
