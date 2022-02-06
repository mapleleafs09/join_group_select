import psycopg2
import sqlalchemy

engine = sqlalchemy.create_engine('postgresql://py47:123456@localhost:5432/py47_db')

connection = engine.connect()

connection.execute("""DELETE FROM Collections""")
connection.execute("""DELETE FROM Singergenre""")
connection.execute("""DELETE FROM Albumsinger""")
connection.execute("""DELETE FROM Genreinfo""")
connection.execute("""DELETE FROM Singerinfo""")
connection.execute("""DELETE FROM Trackinfo""")
connection.execute("""DELETE FROM Albuminfo""")
connection.execute("""DELETE FROM Collectioninfo""")

connection.execute("""INSERT INTO Singerinfo
    VALUES(1, 'Drake');
""")
connection.execute("""INSERT INTO Singerinfo
    VALUES(2, 'Jay-Z');
""")
connection.execute("""INSERT INTO Singerinfo
    VALUES(3, 'Rihanna');
""")
connection.execute("""INSERT INTO Singerinfo
    VALUES(4, 'Scorpions');
""")
connection.execute("""INSERT INTO Singerinfo
    VALUES(5, 'Avicii');
""")
connection.execute("""INSERT INTO Singerinfo
    VALUES(6, 'Calvin Harris');
""")
connection.execute("""INSERT INTO Singerinfo
    VALUES(7, 'Yuri Shevchuk');
""")
connection.execute("""INSERT INTO Singerinfo
    VALUES(8, 'Kanye West');
""")



connection.execute("""INSERT INTO Genreinfo
    VALUES(1, 'Rap');
""")
connection.execute("""INSERT INTO Genreinfo
    VALUES(2, 'Rock');
""")
connection.execute("""INSERT INTO Genreinfo
    VALUES(3, 'Pop');
""")
connection.execute("""INSERT INTO Genreinfo
    VALUES(4, 'Jazz');
""")
connection.execute("""INSERT INTO Genreinfo
    VALUES(5, 'EDM');
""")


connection.execute("""INSERT INTO Albuminfo
    VALUES(1, 'Motion', 2015);
""")

connection.execute("""INSERT INTO Albuminfo
    VALUES(2, 'Anti', 2016);
""")

connection.execute("""INSERT INTO Albuminfo
    VALUES(3, 'Scorpion', 2020);
""")

connection.execute("""INSERT INTO Albuminfo
    VALUES(4, 'The Life of Pablo', 2016);
""")

connection.execute("""INSERT INTO Albuminfo
    VALUES(5, 'Propavshy bez vesti', 2005);
""")

connection.execute("""INSERT INTO Albuminfo
    VALUES(6, 'The Blueprint 3', 2009);
""")

connection.execute("""INSERT INTO Albuminfo
    VALUES(7, 'True', 2013);
""")

connection.execute("""INSERT INTO Albuminfo
    VALUES(8, 'Crazy World', 1990);
""")


connection.execute("""INSERT INTO Albumsinger
    VALUES(1, 6);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(2, 3);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(2, 1);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(3, 1);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(3, 2);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(4, 8);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(4, 3);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(5, 7);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(6, 2);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(7, 5);
""")
connection.execute("""INSERT INTO Albumsinger
    VALUES(8, 4);
""")


connection.execute("""INSERT INTO Singergenre
    VALUES(1, 1);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(1, 2);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(1, 3);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(1, 8);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(2, 4);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(2, 7);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(3, 3);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(4, 7);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(5, 6);
""")
connection.execute("""INSERT INTO Singergenre
    VALUES(5, 5);
""")

connection.execute("""INSERT INTO Trackinfo
    VALUES(1, 7, 'Wake me up', '00:04:09' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(2, 7, 'Hey Brother', '00:04:19' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(3, 7, 'Addicted to you', '00:02:28' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(4, 8, 'Wind of Change', '00:05:10' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(5, 8, 'Crazy World', '00:05:08' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(6, 6, 'Thank You', '00:04:10' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(7, 6, 'Off That', '00:04:06' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(8, 5, 'Propavshy bez vesti', '00:04:28' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(9, 4, 'Famous', '00:03:16' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(10, 4, 'Highlights', '00:03:19' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(11, 3, 'Nonstop', '00:03:58' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(12, 3, 'Talk Up', '00:03:43' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(13, 2, 'Desperado', '00:02:28' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(14, 1, 'My Faith', '00:03:29' );
""")
connection.execute("""INSERT INTO Trackinfo
    VALUES(15, 1, 'Summer', '00:03:42' );
""")

connection.execute("""INSERT INTO Collectioninfo
    VALUES(1, 'Relax' , 2019  );
""")
connection.execute("""INSERT INTO Collectioninfo
    VALUES(2, 'Running' , 2020  );
""")
connection.execute("""INSERT INTO Collectioninfo
    VALUES(3, 'Romantic' , 2015  );
""")
connection.execute("""INSERT INTO Collectioninfo
    VALUES(4, 'My Playlist' , 2021  );
""")
connection.execute("""INSERT INTO Collectioninfo
    VALUES(5, 'Taxi' , 2019  );
""")

connection.execute("""INSERT INTO Collections
    VALUES(1, 4  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(1, 8  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(1, 5  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(1, 13  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(2, 2  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(2, 3  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(2, 15  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(2, 14  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(3, 6  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(3, 11  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(4, 10  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(4, 12  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(4, 2  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(5, 1  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(5, 3  );
""")
connection.execute("""INSERT INTO Collections
    VALUES(5, 7  );
""")

connection.execute("""INSERT INTO Collections
    VALUES(5, 11  );
""")



