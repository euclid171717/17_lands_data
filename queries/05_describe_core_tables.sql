-- Column layout for core objects (one DESCRIBE per statement).
-- Safe width for the editor; avoids SELECT * on huge draft/game tables.

DESCRIBE raw.draft;
DESCRIBE raw.game;
DESCRIBE raw_helpers.dungeon;
DESCRIBE raw_card_data.abilities;
DESCRIBE raw_card_data.cards;
