-- read_checked (design-v2 §6.11.4): an atomic trim check plus a non-blocking
-- XREADGROUP. No XADD, and therefore no MAXLEN trim, can fall between them.
--
-- KEYS[1] stream
-- ARGV[1] group, ARGV[2] consumer, ARGV[3] count
--
-- Returns one of:
--   {'nogroup'}
--   {'gap', first_entry_id, entries_added, length, last_generated_id}
--                                      (group state unchanged)
--   {'ok', entries_added}                (no entries)
--   {'ok', entries_added, <XREADGROUP reply>}
--
-- entries_added rides on every reply but 'nogroup', for the worker's input
-- rate (firehose throughput design §2.2).

local stream, group, consumer, count = KEYS[1], ARGV[1], ARGV[2], ARGV[3]

local function field(flat, name)
  for i = 1, #flat, 2 do
    if flat[i] == name then
      return flat[i + 1]
    end
  end
  return nil
end

-- Compare two decimal strings of arbitrary size.
local function cmp_num(a, b)
  if #a ~= #b then
    return #a < #b and -1 or 1
  end
  if a == b then
    return 0
  end
  return a < b and -1 or 1
end

-- Compare two stream ids "ms-seq".
local function cmp_id(a, b)
  local ams, aseq = string.match(a, '^(%d+)-(%d+)$')
  local bms, bseq = string.match(b, '^(%d+)-(%d+)$')
  local c = cmp_num(ams, bms)
  if c ~= 0 then
    return c
  end
  return cmp_num(aseq, bseq)
end

if redis.call('EXISTS', stream) == 0 then
  return {'nogroup'}
end

local groups = redis.call('XINFO', 'GROUPS', stream)
local g = nil
for _, entry in ipairs(groups) do
  if field(entry, 'name') == group then
    g = entry
    break
  end
end
if g == nil then
  return {'nogroup'}
end

local info = redis.call('XINFO', 'STREAM', stream)
local length = tonumber(field(info, 'length'))
local added = field(info, 'entries-added')
local first = field(info, 'first-entry')
local last_delivered = field(g, 'last-delivered-id')
local entries_read = field(g, 'entries-read')

local first_id = nil
if first and type(first) == 'table' then
  first_id = first[1]
end

-- lag = entries-added - entries-read (§6.11.4, #154), computed from the
-- group's own counter. The XINFO GROUPS `lag` field cannot be used: from
-- Redis 7.4, when last-delivered-id is before the first entry, Redis
-- re-estimates it as `length`, so `lag > length` never holds. An invalid
-- (nil) counter is a gap, as a nil lag is.
local gap = false
if not entries_read then
  gap = true
elseif first_id and cmp_id(first_id, last_delivered) > 0
    and tonumber(added) - tonumber(entries_read) > length then
  gap = true
end
if gap then
  return {'gap', first_id or '', tostring(added), tostring(length), field(info, 'last-generated-id')}
end

local reply = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', count, 'STREAMS', stream, '>')
if not reply then
  return {'ok', tostring(added)}
end
return {'ok', tostring(added), reply}
