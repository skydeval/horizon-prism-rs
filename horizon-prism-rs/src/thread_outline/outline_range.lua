-- outline_range (thread outline design §3.3 step 3, R4-1): one replay chunk
-- with the stream's first retained id, read atomically, as read_checked
-- reads its trim evidence. No XADD, and so no MAXLEN trim, can fall between
-- the two reads.
--
-- KEYS[1] stream
-- ARGV[1] exclusive start id, ARGV[2] exclusive end id, ARGV[3] count
--
-- Returns {first_retained_id or '', <XRANGE reply>}.

local stream, start, stop, count = KEYS[1], ARGV[1], ARGV[2], ARGV[3]

local head = redis.call('XRANGE', stream, '-', '+', 'COUNT', 1)
local first = ''
if head[1] then
  first = head[1][1]
end
local entries = redis.call('XRANGE', stream, '(' .. start, '(' .. stop, 'COUNT', count)
return {first, entries}
