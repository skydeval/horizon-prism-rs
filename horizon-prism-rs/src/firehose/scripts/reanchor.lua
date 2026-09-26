-- reanchor (design-v2 §6.11.4): after a gap transaction commits, skip the lost
-- range explicitly and make entries-read exact again.
--
-- KEYS[1] stream
-- ARGV[1] group
--
-- XGROUP SETID <stream> <group> <predecessor of first-entry-id>
--   ENTRIESREAD <entries-added - length>
-- The predecessor of ms-seq is ms-(seq-1), or (ms-1)-18446744073709551615
-- when seq = 0. An empty stream anchors at $.
-- Returns {new_id, entries_read}.

local stream, group = KEYS[1], ARGV[1]

local function field(flat, name)
  for i = 1, #flat, 2 do
    if flat[i] == name then
      return flat[i + 1]
    end
  end
  return nil
end

-- Decrement a non-negative decimal string by one (it must be > 0).
local function dec(s)
  local digits = {}
  for i = 1, #s do
    digits[i] = string.byte(s, i) - 48
  end
  local i = #digits
  while digits[i] == 0 do
    digits[i] = 9
    i = i - 1
  end
  digits[i] = digits[i] - 1
  local out = {}
  local started = false
  for j = 1, #digits do
    if digits[j] ~= 0 or started or j == #digits then
      started = true
      out[#out + 1] = string.char(digits[j] + 48)
    end
  end
  return table.concat(out)
end

local info = redis.call('XINFO', 'STREAM', stream)
local length = tonumber(field(info, 'length'))
local added = tonumber(field(info, 'entries-added'))
local first = field(info, 'first-entry')
local entries_read = added - length

local target = '$'
if first and type(first) == 'table' then
  local ms, seq = string.match(first[1], '^(%d+)-(%d+)$')
  if seq ~= '0' then
    target = ms .. '-' .. dec(seq)
  elseif ms ~= '0' then
    target = dec(ms) .. '-18446744073709551615'
  else
    target = '0-0'
  end
end

redis.call('XGROUP', 'SETID', stream, group, target, 'ENTRIESREAD', entries_read)
return {target, tostring(entries_read)}
