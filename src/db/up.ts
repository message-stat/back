// generate knex migratoion 

export const up = `
create table if not exists Word (
  dateTime DateTime64,
  text String,
  userId FixedString(64),
  position Enum8('first' = 0, 'begin' = 1, 'center' = 2, 'end' = 3, 'last' = 4),
  messageLength Enum8('single' = 0, 'short' = 1, 'medium' = 2, 'long' = 3),
  debug String
) engine = MergeTree()
  order by (userId, intHash32(toUInt32(dateTime)))
  partition by userId
  sample by intHash32(toUInt32(dateTime));

create table if not exists Message (
  dateTime DateTime64,
  userId FixedString(64),
  chatId FixedString(64),
  isChat UInt8,
  words Int16,
  symbols Int16,
  timeFromLastSend Int32,
  timeFromLastReceive Int32
) engine = MergeTree()
  order by (userId, intHash32(toUInt32(dateTime)))
  partition by userId
  sample by intHash32(toUInt32(dateTime));
`
