-- Se ejecutaron en distintas querys:
ALTER TABLE rick_and_morty_v3 DROP COLUMN "origin";

-- Luego 'location'
ALTER TABLE rick_and_morty_v3 DROP COLUMN "location";

-- Luego 'episode'
ALTER TABLE rick_and_morty_v3 DROP COLUMN "episode";

-- Luego 'url'
ALTER TABLE rick_and_morty_v3 DROP COLUMN "url";

-- Finalmente, eliminar la columna 'image'
ALTER TABLE rick_and_morty_v3 DROP COLUMN "image";
