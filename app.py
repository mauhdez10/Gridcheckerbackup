"""
Broadcast Time Calculator — Page 1
Functions: Segment Adder | End Break Calculator | Segment Calculator
"""
import streamlit as st
from datetime import datetime, timedelta
import re

st.set_page_config(page_title='Time Calculator', layout='wide', page_icon='⏱')

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
:root {
  --bg:#0e1117;--surface:#1a1d26;--surface2:#22263a;
  --accent:#00d4ff;--accent2:#ff6b35;--accent3:#7fff6b;
  --text:#e8ecf0;--muted:#6b7280;--border:#2d3148;
}
html,body,.stApp{background:var(--bg);color:var(--text);font-family:'IBM Plex Sans',sans-serif;}
h1,h2,h3{font-family:'JetBrains Mono',monospace;letter-spacing:-0.5px;}
.stButton>button{background:var(--surface2);border:1px solid var(--border);color:var(--accent);
  font-family:'JetBrains Mono',monospace;border-radius:4px;font-size:0.8rem;
  letter-spacing:1px;text-transform:uppercase;transition:all 0.15s;}
.stButton>button:hover{background:var(--accent);color:var(--bg);border-color:var(--accent);}
.result-box{background:var(--surface);border:1px solid var(--accent);border-radius:6px;padding:14px 18px;margin:8px 0;font-family:'JetBrains Mono',monospace;}
.result-big{font-size:1.6rem;font-weight:700;color:var(--accent);}
.result-sub{font-size:0.85rem;color:var(--muted);margin-top:4px;}
.section-header{border-bottom:1px solid var(--border);padding-bottom:8px;margin-bottom:16px;
  font-family:'JetBrains Mono',monospace;font-size:0.75rem;letter-spacing:2px;text-transform:uppercase;color:var(--muted);}
.tc-badge{display:inline-block;background:var(--surface2);border:1px solid var(--border);
  padding:4px 10px;border-radius:4px;font-family:'JetBrains Mono',monospace;font-size:0.9rem;color:var(--accent3);}
</style>
""", unsafe_allow_html=True)

FPS = 29.97

def tc_to_secs(tc_str):
    tc_str = tc_str.strip()
    m = re.match(r'^(\d+):(\d+):(\d+)[;:](\d+)$', tc_str)
    if m:
        h,mi,s,f = int(m.group(1)),int(m.group(2)),int(m.group(3)),int(m.group(4))
        return h*3600 + mi*60 + s + f/FPS
    m = re.match(r'^(\d+):(\d+):(\d+)$', tc_str)
    if m:
        h,mi,s = int(m.group(1)),int(m.group(2)),int(m.group(3))
        return h*3600 + mi*60 + s
    m = re.match(r'^(\d+):(\d+)$', tc_str)
    if m:
        mi,s = int(m.group(1)),int(m.group(2))
        return mi*60 + s
    return None

def secs_to_tc(total_secs, with_frames=True):
    total_secs = max(0, float(total_secs))
    frames = round((total_secs % 1) * FPS)
    if frames >= 30: frames = 29
    total_int = int(total_secs)
    h  = total_int // 3600
    mi = (total_int % 3600) // 60
    s  = total_int % 60
    if with_frames: return f'{h:02d}:{mi:02d}:{s:02d};{frames:02d}'
    return f'{h:02d}:{mi:02d}:{s:02d}'

def fmt_hms(secs, use_ampm):
    tc = secs_to_tc(secs % 86400, with_frames=False)
    if not use_ampm: return tc
    h,m,s = map(int, tc.split(':'))
    suffix = 'AM' if h < 12 else 'PM'
    return f'{h%12 or 12}:{m:02d}:{s:02d} {suffix}'

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('# ⏱ BROADCAST TIME CALCULATOR')
st.markdown('<div class="section-header">Segment Adder · End Break Calc · Segment Calculator</div>', unsafe_allow_html=True)

col_tz, col_fmt, col_off = st.columns(3)
with col_tz:   tz_choice = st.selectbox('Timezone display', ['ET (Eastern)', 'UTC only'])
with col_fmt:  time_fmt  = st.selectbox('Time format', ['Military (24h)', 'AM/PM'])
with col_off:  et_offset = st.number_input('UTC offset hrs', value=-4, min_value=-12, max_value=14, help='EDT=-4  EST=-5')
use_ampm = time_fmt == 'AM/PM'
show_et  = 'ET' in tz_choice

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# FUNCTION 1 — SEGMENT ADDER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('## 01 — SEGMENT ADDER')
st.caption('Enter segment durations in HH:MM:SS;FF · 29.97fps · Total shown as timecode')

if 'seg_rows' not in st.session_state:
    st.session_state.seg_rows = [{'label': f'Segment {i+1}', 'tc': ''} for i in range(3)]

ca, cb = st.columns([1, 1])
with ca:
    if st.button('＋ Add segment'):
        n = len(st.session_state.seg_rows) + 1
        st.session_state.seg_rows.append({'label': f'Segment {n}', 'tc': ''})
        st.rerun()
with cb:
    if st.button('✕ Clear all'):
        st.session_state.seg_rows = [{'label': f'Segment {i+1}', 'tc': ''} for i in range(3)]
        st.rerun()

total_secs = 0.0
valid_count = 0
to_delete = None

for idx, row in enumerate(st.session_state.seg_rows):
    c1, c2, c3, c4 = st.columns([2, 2, 1.5, 0.5])
    with c1:
        lbl = st.text_input('Label', value=row['label'], key=f'lbl_{idx}', label_visibility='collapsed', placeholder=f'Segment {idx+1}')
        st.session_state.seg_rows[idx]['label'] = lbl
    with c2:
        tc_val = st.text_input('TC', value=row['tc'], key=f'tc_{idx}', label_visibility='collapsed', placeholder='00:00:00;00')
        st.session_state.seg_rows[idx]['tc'] = tc_val
    with c3:
        secs = tc_to_secs(tc_val) if tc_val.strip() else None
        if secs is not None:
            total_secs += secs; valid_count += 1
            st.markdown(f'<span class="tc-badge">{secs_to_tc(secs)}</span>', unsafe_allow_html=True)
        elif tc_val.strip():
            st.markdown('⚠️ invalid')
    with c4:
        if st.button('🗑', key=f'del_{idx}'):
            to_delete = idx

if to_delete is not None:
    st.session_state.seg_rows.pop(to_delete); st.rerun()

if valid_count > 0:
    tc_result = secs_to_tc(total_secs)
    utc_d = fmt_hms(total_secs % 86400, use_ampm)
    et_d  = fmt_hms((total_secs + et_offset*3600) % 86400, use_ampm)
    sub   = f'UTC: {utc_d}' + (f'  |  ET: {et_d}' if show_et else '') + f'  |  {valid_count} segments'
    st.markdown(f'<div class="result-box"><div class="result-big">{tc_result}</div><div class="result-sub">{sub}</div></div>', unsafe_allow_html=True)
    if st.button('📋 Copy total timecode'):
        st.session_state['_tc_copy'] = tc_result
    if st.session_state.get('_tc_copy'):
        st.code(st.session_state['_tc_copy'])
        st.caption('Select all and copy ↑')

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# FUNCTION 2 — END BREAK CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('## 02 — END BREAK CALCULATOR')
st.caption('Cue time = Out time − Break duration')

c1, c2 = st.columns(2)
with c1:
    st.markdown('**Out time (end of live)**')
    r1, r2, r3 = st.columns(3)
    out_h = r1.number_input('H', 0, 23, 18, key='oh', format='%02d', label_visibility='collapsed')
    out_m = r2.number_input('M', 0, 59, 0,  key='om', format='%02d', label_visibility='collapsed')
    out_s = r3.number_input('S', 0, 59, 0,  key='os', format='%02d', label_visibility='collapsed')
    r1.caption('Hour'); r2.caption('Min'); r3.caption('Sec')
    out_tz = st.selectbox('Out time timezone', ['ET', 'UTC'], key='out_tz2')
with c2:
    st.markdown('**Break duration**')
    b1, b2 = st.columns(2)
    brk_m = b1.number_input('Minutes', 0, 59, 3, key='bm2')
    brk_s = b2.number_input('Seconds', 0, 59, 45, key='bs2')

out_secs = out_h*3600 + out_m*60 + out_s
out_utc  = (out_secs - et_offset*3600) % 86400 if out_tz == 'ET' else out_secs % 86400
brk_dur  = brk_m*60 + brk_s
cue_utc  = (out_utc - brk_dur) % 86400
cue_et   = (cue_utc + et_offset*3600) % 86400

cue_utc_s = fmt_hms(cue_utc, use_ampm)
cue_et_s  = fmt_hms(cue_et,  use_ampm)
out_utc_s = fmt_hms(out_utc, use_ampm)
out_et_s  = fmt_hms(out_utc + et_offset*3600, use_ampm)

if show_et:
    cue_main = f'{cue_et_s} ET  /  {cue_utc_s} UTC'
    out_line = f'Out: {out_et_s} ET / {out_utc_s} UTC'
else:
    cue_main = f'{cue_utc_s} UTC'
    out_line = f'Out: {out_utc_s} UTC'

st.markdown(f'<div class="result-box"><div style="color:var(--muted);font-size:0.8rem;margin-bottom:4px;">CUE TIME (leave segment)</div><div class="result-big">{cue_main}</div><div class="result-sub">{out_line}  −  Break {brk_m}m {brk_s:02d}s</div></div>', unsafe_allow_html=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# FUNCTION 3 — SEGMENT CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('## 03 — SEGMENT CALCULATOR')
st.caption('Enter durations as HH:MM:SS or MM:SS')

c1, c2 = st.columns(2)
with c1:
    brk_total = st.text_input('Row 1 — Total break time',         placeholder='00:08:00', key='sc1')
    segs_done = st.text_input('Row 2 — Total current segments',   placeholder='00:24:00', key='sc2')
with c2:
    trt_inp   = st.text_input('Row 3 — TRT (Total Run Time)',     placeholder='01:00:00', key='sc3')
    seg_cnt   = st.number_input('Number of remaining segments', 1, 20, 3, key='sc4')

brk_s3  = tc_to_secs(brk_total) or 0
segs_s3 = tc_to_secs(segs_done) or 0
trt_s3  = tc_to_secs(trt_inp)

if trt_s3 and trt_s3 > 0:
    used   = brk_s3 + segs_s3
    remain = max(0, trt_s3 - used)
    sugg   = remain / seg_cnt if seg_cnt > 0 else 0
    pct    = used / trt_s3 * 100
    st.markdown(f"""
    <div class="result-box">
      <div style="color:var(--muted);font-size:0.8rem;margin-bottom:4px;">ROW 4 — TIME REMAINING</div>
      <div class="result-big">{secs_to_tc(remain, with_frames=False)}</div>
      <div class="result-sub">
        Used: {secs_to_tc(used, with_frames=False)} ({pct:.0f}%) of TRT {secs_to_tc(trt_s3, with_frames=False)}<br>
        Suggestion: <strong>{seg_cnt} segments × ~{secs_to_tc(sugg, with_frames=False)}</strong> each
      </div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.info('Enter TRT to calculate.')

st.markdown('<div style="margin-top:40px;text-align:center;color:var(--muted);font-size:0.75rem;font-family:JetBrains Mono,monospace;">© 2026 Mauricio Hernandez · Broadcast Operations Toolkit</div>', unsafe_allow_html=True)
