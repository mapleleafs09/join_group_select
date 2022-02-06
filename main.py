import psycopg2
import sqlalchemy

engine = sqlalchemy.create_engine('postgresql://py47:123456@localhost:5432/py47_db')
connection = engine.connect()

# Запрос 1 количество исполнителей в каждом жанре

count_singer_in_genre = connection.execute("""SELECT gi.name, COUNT(sg.genre_id) FROM Genreinfo gi
     JOIN Singergenre sg ON gi.id = sg.genre_id
     GROUP BY gi.name
     ;""").fetchall()
# print(count_singer_in_genre)

# Запрос 2 количество треков, вошедших в альбомы 2019-2020 годов

track_in_album_19_20 = connection.execute("""SELECT ai.name, COUNT(ti.id) FROM Trackinfo ti
     JOIN Albuminfo ai ON ti.album_id = ai.id
     WHERE ai.year BETWEEN 2019 AND 2020
     GROUP BY ai.name
     ;""").fetchall()
# print(Track_in_album_19_20)

# Запрос 3 средняя продолжительность треков по каждому альбому

average_duration_by_album = connection.execute("""SELECT ai.name, AVG(duration) FROM Trackinfo ti
     JOIN Albuminfo ai ON ti.album_id = ai.id
     GROUP BY ai.name
      ;""").fetchall()
# print(average_duration_by_album)

# Запрос 4 все исполнители, которые не выпустили альбомы в 2020 году

singer_album_not_2020 = connection.execute("""SELECT si.name  FROM Singerinfo si
     JOIN Albumsinger asi ON asi.singer_id = si.id
     JOIN Albuminfo ai ON ai.id = asi.album_id
     WHERE si.name NOT IN (
        SELECT si.name FROM Singerinfo si
        JOIN Albumsinger asi ON asi.singer_id = si.id
        JOIN Albuminfo ai ON ai.id = asi.album_id
        WHERE ai.year = 2020)
     ;""").fetchall()
# print(singer_album_not_2020)

# Запрос 5 названия сборников, в которых присутствует конкретный исполнитель (выберите сами)

collection_with_singer = connection.execute("""SELECT DISTINCT ci.name FROM Singerinfo si
     JOIN Albumsinger asi ON asi.singer_id = si.id
     JOIN Albuminfo ai ON ai.id = asi.album_id
     JOIN Trackinfo ti ON ti.album_id = ai.id
     JOIN Collections c ON c.track_id = ti.id
     JOIN Collectioninfo ci ON ci.id = c.collection_id
     WHERE si.name = 'Avicii'
     ;""").fetchall()
# print(collection_with_singer)

# Запрос 6 название альбомов, в которых присутствуют исполнители более 1 жанра

albums_with_singer_more1_genre = connection.execute("""SELECT ai.name  FROM Albuminfo ai
    JOIN Albumsinger asi ON asi.album_id = ai.id
    JOIN Singerinfo si ON si.id = asi.singer_id
    JOIN Singergenre sg ON sg.singer_id = si.id
    JOIN Genreinfo gi ON gi.id = sg.genre_id
    GROUP BY ai.name
    HAVING COUNT(gi.name) > 1
    ;""").fetchall()
# print(albums_with_singer_more1_genre)

# Запрос 7 наименование треков, которые не входят в сборники

tracks_without_collection = connection.execute("""SELECT ti.name FROM Trackinfo ti
    LEFT JOIN Collections c ON c.track_id = ti.id
    WHERE c.collection_id IS NULL
    ;""").fetchall()
# print(tracks_without_collection)

# Запрос 8 исполнителя(-ей), написавшего самый короткий по продолжительности трек (теоретически таких треков может быть несколько);

singer_shortest_track = connection.execute("""SELECT ti.duration, si.name, ti.name FROM Trackinfo ti
    JOIN Albuminfo ai ON ai.id = ti.album_id
    JOIN Albumsinger asi ON asi.album_id = ai.id
    JOIN Singerinfo si ON si.id = asi.singer_id
    WHERE ti.duration = (SELECT MIN(duration) FROM Trackinfo)
    ORDER BY ti.duration
    ;""").fetchall()
# print(singer_shortest_track)

# Запрос 9 название альбомов, содержащих наименьшее количество треков

albums_min_tracks = connection.execute("""SELECT ai.name, COUNT(ti.id) FROM Albuminfo ai
    JOIN Trackinfo ti ON ti.album_id = ai.id
    GROUP BY ai.name
    HAVING COUNT(ti.id) = (
        SELECT COUNT(ti.id) FROM Albuminfo ai
        JOIN Trackinfo ti ON ti.album_id = ai.id
        GROUP BY ai.name
        ORDER BY COUNT(ti.id)
        LIMIT 1)
    ;""").fetchall()
# print(albums_min_tracks)