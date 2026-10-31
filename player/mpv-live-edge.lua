-- mpv script for watching the stream live: keeps playback at the end of what
-- has come in (the live edge).
--
--   mpv --profile=low-latency --hwdec=auto --audio-buffer=0.05 \
--       --script=player/mpv-live-edge.lua "udp://127.0.0.1:25566?overrun_nonfatal=1&fifo_size=1024"
--
-- mpv does not return to the live edge on its own: after a pause, a stall or
-- a seek back it keeps playing that far behind. This script
--   * catches up a small lag (up to catch_up_limit) by playing a bit faster;
--   * leaves a larger lag alone, since that is a seek back on purpose, and
--     shows how far behind live the picture is;
--   * jumps to the live edge on End or Ctrl+Right (opens the stream again),
--     and on a forward seek (Right, Up, Shift+Right) that would go past it.
-- It acts only on udp:// and srt:// streams.

local options = {
    target_lag = 0.15,     -- seconds behind the edge that count as live
    catch_up_limit = 1.0,  -- larger lags are a seek back on purpose
    catch_up_speed = 1.1,
    check_interval = 0.25,
}
require("mp.options").read_options(options, "live-edge")

local catching_up = false

local function is_live_stream()
    local path = mp.get_property("path", "")
    return path:match("^udp://") ~= nil or path:match("^srt://") ~= nil
end

-- lag returns how far playback is behind the end of the cache, in seconds;
-- nil while either is unknown.
local function lag()
    local position = mp.get_property_number("time-pos")
    local state = mp.get_property_native("demuxer-cache-state")
    if position == nil or state == nil or state["cache-end"] == nil then
        return nil
    end
    return state["cache-end"] - position
end

local function set_catching_up(on)
    if on ~= catching_up then
        catching_up = on
        mp.set_property_number("speed", on and options.catch_up_speed or 1)
    end
end

local function check()
    if not is_live_stream() or mp.get_property_bool("pause") then
        set_catching_up(false)
        return
    end
    local behind = lag()
    if behind == nil then
        return
    end
    if behind > options.catch_up_limit then
        set_catching_up(false)
        mp.osd_message(string.format("%.1f s behind live, End or Ctrl+Right to return", behind), options.check_interval * 2)
    elseif behind > options.target_lag then
        set_catching_up(true)
    elseif behind < options.target_lag / 2 then
        set_catching_up(false)
    end
end

-- jump_to_live opens the stream again, which starts it at the live edge:
-- UDP has no connection, the client keeps sending, and the picture comes
-- with the next keyframe (within a second). Seeking forward to the edge did
-- not get there: mpv refuses absolute seeks in a stream it does not see as
-- seekable, and a relative one landed about a second short of the edge.
local function jump_to_live()
    set_catching_up(false)
    mp.commandv("loadfile", mp.get_property("path"), "replace")
    mp.osd_message("live", 1)
end

-- seek_forward replaces mpv's forward seek keys. mpv cannot seek past what
-- has come in ("Cannot seek in this stream"), so a step that would go past
-- the live edge jumps to live instead; a lag the catch-up handles is left
-- to it.
local function seek_forward(seconds, flags)
    return function()
        local behind = lag()
        if not is_live_stream() or behind == nil or seconds < behind then
            mp.commandv("seek", tostring(seconds), flags)
        elseif behind > options.catch_up_limit then
            jump_to_live()
        end
    end
end

mp.add_periodic_timer(options.check_interval, check)
mp.add_key_binding("END", "jump-to-live", jump_to_live)
-- for keyboards without End; Ctrl+Right is the subtitle seek, of no use in the stream
mp.add_key_binding("Ctrl+RIGHT", "jump-to-live-ctrl-right", jump_to_live)
-- the forward seeks of mpv's default input.conf
mp.add_key_binding("RIGHT", "seek-forward-5", seek_forward(5, "relative"), { repeatable = true })
mp.add_key_binding("UP", "seek-forward-60", seek_forward(60, "relative"), { repeatable = true })
mp.add_key_binding("Shift+RIGHT", "seek-forward-1", seek_forward(1, "relative+exact"), { repeatable = true })
