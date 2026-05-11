# Frame Analysis Guide

## Purpose

Convert extracted frames into stable, reusable visual observations for short-video analysis.

Use this guide after frame extraction in both routes:
- `audio-sop`
- `keyframe-sop`

This guide is not a separate workflow.
It is a reference for how to analyse frames once they have already been extracted.
Use it in Part 4.1, and then pass the selected start/end frame analyses into Part 4.2 for multimodal integration.

---

## Part 1. What to analyse in a single frame

For each candidate frame, analyse the image from the following angles.
Do not stop at vague descriptions such as `a person indoors` or `a product on a table`.
The goal is to identify what the frame is doing in the segment.

### 1. Main subject
Identify the main subject and separate it from secondary or background elements.

Check:
- who or what is the main subject
- whether there are secondary subjects
- which elements are only background context

Suggested fields:
- `main_subject`
- `secondary_subjects`
- `background_elements`

### 2. Narrative role
If the frame contains a person, object, UI, or text card, judge its narrative role in the video.
Do not guess real-world identity. Judge only the storytelling role.

Common roles:
- presenter
- demonstrator
- experiencer
- interview subject
- hand/operator
- product-as-main-subject
- UI-as-main-subject
- reaction shot subject

Suggested field:
- `narrative_role`

### 3. Expression and emotion
If a face or body pose is visible, analyse emotional and expressive signals.

Check:
- facial expression
- emotional tone
- gaze direction if visible
- expression intensity if relevant

Suggested fields:
- `facial_expression`
- `emotional_tone`
- `gaze_direction`

### 4. Action
Identify what the subject is doing now.
Action is one of the most important fields.

Check:
- current action
- action target
- action direction or movement trend if obvious
- action result if visible in the frame

Suggested fields:
- `current_action`
- `action_target`
- `action_result`

### 5. Scene and props
Identify the environment and useful props.

Check:
- scene type
- props / tools / products / devices
- whether it is a real-world scene, studio scene, screen recording, or text card

Suggested fields:
- `scene_type`
- `props`

### 6. Shot and composition
Describe how the frame is presented visually.

Check:
- shot type: wide / medium / close-up / extreme close-up
- camera angle: eye-level / top-down / side / selfie / screen-recording
- composition: centered, foreground emphasis, background blur, split layout, text overlay

Suggested fields:
- `shot_type`
- `camera_angle`
- `composition_notes`

### 7. On-screen text and visible labels
Capture text that materially affects interpretation.

Examples:
- subtitles
- labels
- buttons
- prices
- product names
- UI prompts
- metric values

Suggested field:
- `on_screen_text`

### 8. Visual focus and information carrier
Identify where the viewer's attention should go and what actually carries the information.
The main subject is not always the information carrier.

Examples:
- the person's face is central, but the real information is in the subtitle
- a phone is visible, but the real information is the popup button
- a presenter is speaking, but the key information is the product label held near camera

Suggested fields:
- `visual_focus`
- `information_carrier`

### 9. Frame summary
Finish with one concise summary sentence.

Suggested field:
- `frame_summary`

---

## Part 2. Start-end frame comparison within one segment

After analysing single frames, compare the chosen start frame and end frame for the same segment.
The purpose is to understand what changed across the segment.

### Compare these dimensions

#### 1. State change
What is different between the beginning and the end?

Examples:
- unopened product → opened product
- neutral face → surprised face
- empty screen → result screen
- hand preparing action → action completed

Suggested field:
- `state_change`

#### 2. Action progression
How did the action move forward?

Examples:
- introduction → demonstration
- holding → applying
- searching → selecting
- approaching camera → showing detail

Suggested field:
- `action_progression`

#### 3. Focus shift
Did the visual focus move?

Examples:
- person face → product detail
- wide context → close detail
- speaker → subtitle card
- interface overview → CTA button

Suggested field:
- `focus_shift`

#### 4. Segment function guess
Judge what role this segment plays in the overall script.

Common values:
- hook
- setup
- demo
- comparison
- proof
- reaction
- CTA
- ending
- transition

Suggested field:
- `segment_function_guess`

### Suggested start-end summary fields
- `start_state_summary`
- `end_state_summary`
- `state_change`
- `action_progression`
- `focus_shift`
- `segment_function_guess`

---

## Part 3. Minimal structured output

Use a light structure. Do not turn this into a heavy schema.
Only keep the fields that help downstream script-table writing.

### Frame-level
- `frame_id`
- `timestamp`
- `main_subject`
- `secondary_subjects`
- `narrative_role`
- `facial_expression`
- `emotional_tone`
- `current_action`
- `scene_type`
- `props`
- `shot_type`
- `camera_angle`
- `on_screen_text`
- `visual_focus`
- `information_carrier`
- `frame_summary`

### Segment-level delta
- `start_frame_id`
- `end_frame_id`
- `start_state_summary`
- `end_state_summary`
- `state_change`
- `action_progression`
- `focus_shift`
- `segment_function_guess`

---

## Part 4. Anti-error rules

### Do not do these things
- do not guess a person's real identity
- do not over-infer from blurry frames
- do not mistake background objects for the main subject
- do not stop at low-information descriptions
- do not ignore subtitles, labels, buttons, prices, product names, or UI text
- do not analyse a segment only from one isolated frame when start-end comparison is available

### Prioritization rule
If the frame contains many visible elements, prioritize in this order:
1. information carrier
2. main subject
3. action
4. visible text
5. scene and composition

### Practical test
A good frame analysis should help answer these five questions:
1. who or what is the main subject of this segment?
2. what is it doing?
3. what is the most important information point in the frame?
4. what changed from the start of the segment to the end?
5. what function does this segment serve in the script?

If those five answers are still weak, the frame analysis is not detailed enough.
