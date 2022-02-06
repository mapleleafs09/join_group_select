
create table if not exists Singerinfo (
	id serial primary key,
	name varchar(60) not null
);

create table if not exists Albuminfo (
	id serial primary key,
	name varchar(60) not null,
	year integer not null
);

create table if not exists Albumsinger (
	album_id integer references Albuminfo(id),
	singer_id integer references Singerinfo(id),
	constraint pk primary key (album_id, singer_id)
);


create table if not exists Genreinfo (
	id serial primary key,
	name varchar(60) not null
);

create table if not exists Singergenre (
	genre_id integer references Genreinfo(id),
	singer_id integer references Singerinfo(id),
	constraint pk_singergenre primary key (genre_id, singer_id)
);

create table if not exists Collectioninfo (
	id serial primary key,
	name varchar(60) not null,
	year integer check (year >= 1950)
);

create table if not exists Trackinfo (
	id serial primary key,
	album_id integer references Albuminfo(id),
	name varchar(60) not null,
	duration time not null
);

create table if not exists Collections (
	collection_id integer references Collectioninfo(id),
	track_id integer references Trackinfo(id),
	constraint pk_collections primary key (collection_id, track_id)
);
