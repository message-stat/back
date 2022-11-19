// generate knex migratoion 

export const up = `
create table if not exists Word (
  dateTime DateTime64,
  text String,
  userId FixedString(64),
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
