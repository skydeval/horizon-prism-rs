-- group_create (design-v2 §6.11.4): create the consumer group if it is absent,
-- with ENTRIESREAD set so lag is exact from the start.
--
-- KEYS[1] stream
-- ARGV[1] group
--
-- Returns 1 when the group was created, 0 when it already existed.

local stream, group = KEYS[1], ARGV[1]

local function field(flat, name)
  for i = 1, #flat, 2 do
    if flat[i] == name then
      return flat[i + 1]
    end
  end
  return nil
end

local added = 0
if redis.call('EXISTS', stream) == 1 then
  for _, entry in ipairs(redis.call('XINFO', 'GROUPS', stream)) do
    if field(entry, 'name') == group then
      return 0
    end
  end
  added = tonumber(field(redis.call('XINFO', 'STREAM', stream), 'entries-added'))
end

redis.call('XGROUP', 'CREATE', stream, group, '$', 'MKSTREAM', 'ENTRIESREAD', added)
return 1
