USE chatroom;

CREATE TABLE messages (
  room TEXT,
  id TIMEUUID,
  when TIMESTAMP,
  sender TEXT,
  message TEXT,
  PRIMARY KEY ( (room), id )
) WITH CLUSTERING ORDER BY (id DESC);
