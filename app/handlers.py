from aiogram import Bot, types, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from pyannote.audio import Pipeline
from transformers import pipeline
import app.keyboards as kb
import whisper
import tempfile
import os
from moviepy import VideoFileClip
from pydub import AudioSegment
from app.database import db
from typing import Optional
from TTS.api import TTS
import soundfile as sf
from transformers import AutoModelForSeq2SeqLM
from dotenv import load_dotenv

load_dotenv()

last_text_for_tts = {}

class UserState(StatesGroup):
    waiting_for_tts = State()
    waiting_for_transcribe = State()

tts = TTS("tts_models/multilingual/multi-dataset/xtts_v2")
model = whisper.load_model("medium")
diarization_pipeline = Pipeline.from_pretrained(
    "pyannote/speaker-diarization",
    use_auth_token=os.getenv('HF_TOKEN')
)
router = Router()

last_transcription = {}
last_raw_transcription = {}

def transcribe_and_diarize(audio_file):
    # Транскрибация
    result = model.transcribe(audio_file, fp16=False)
    segments = result["segments"]
    raw_text = result["text"].strip()
    
    # Диаризация
    diarization = diarization_pipeline(audio_file)
    
    # Список интервалов дикторов
    speaker_intervals = []
    for turn, _, speaker in diarization.itertracks(yield_label=True):
        speaker_intervals.append({
            'start': turn.start,
            'end': turn.end,
            'speaker': speaker
        })
    
    speaker_intervals.sort(key=lambda x: x['start'])
    
    final_transcript = []
    current_segments = []
    
    for segment in segments:
        start_time = segment["start"]
        end_time = segment["end"]
        text = segment["text"].strip()
        
        speaker = None
        best_match = None
        best_overlap = 0

        for interval in speaker_intervals:
            overlap = min(end_time, interval['end']) - max(start_time, interval['start'])
            if overlap > best_overlap and overlap > 0:
                best_overlap = overlap
                best_match = interval['speaker']

        speaker = best_match if best_match else "НЕИЗВЕСТНЫЙ_ДИКТОР"
        
        current_segments.append({
            'speaker': speaker,
            'text': text,
            'start': start_time,
            'end': end_time
        })
    
    if current_segments:
        current_speaker = current_segments[0]['speaker']
        combined_text = current_segments[0]['text']
        
        for i in range(1, len(current_segments)):
            if current_segments[i]['speaker'] == current_speaker:
                combined_text += " " + current_segments[i]['text']
            else:
                final_transcript.append(f"{current_speaker}: {combined_text}")
                current_speaker = current_segments[i]['speaker']
                combined_text = current_segments[i]['text']
        
        final_transcript.append(f"{current_speaker}: {combined_text}")
    
    return "\n".join(final_transcript), raw_text

@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        f'*Бот создан для работы с аудио и текстом.*\n'
        f'Имеет следующие функции:\n\n'
        f'💬*Транскрибация* - преобразует аудио и видеофайлы в текст.\nЕго также можно получить из голосового сообщения.\n\n' 
        f'👥*Диаризация* - определяет, кто из спикеров что сказал.\nИдеально для разбора интервью, совещаний и подкастов.\n\n'
        f'✂️*Суммаризация* - сокращает длинные тексты, оставляя только главное.\nПолезно для быстрого ознакомления с контентом без потери смысла.\n\n'
        f'🎙️*Синтез речи* - озвучивает выбранный текст и отправляет аудио файлом. Озвучить можно двумя способами:\n1)Используя готовые голоса\n2)Используя любой голос в качестве образца\n\n'
        f'Если есть какие-либо вопросы по использованию функционала бота, напишите /help',
        reply_markup=kb.main,
        parse_mode="Markdown"
    )

@router.message(Command('help'))
async def get_help(message: Message):
    await message.answer(
        f'Для получения текста и определения спикеров нажмите кнопку *Транскрибация и диаризация*. После этого отправьте аудио/видео файл или голосовое сообщение боту.\n\n'
        f'Для выполнения суммаризации и синтеза речи сохраните полученный вами текст. Затем нажмите на кнопку *Сохраненные тексты* для дальнейшей работы.\n\n'
        f'Для озвучивания текста можно выбрать один из двух вариантов. При выборе готового голоса вы получите аудио файл как бот завершит синтез речи. А для озвучки своим нажмите на образец голоса и отправьте аудио файл или голосовое сообщение с нужным вам голосом боту.\n\n',
        parse_mode="Markdown"
    )

@router.message(F.text == 'Транскрибация и диаризация')
async def get_transcribe(message: Message, state: FSMContext):
    await message.answer("Отправьте мне голосовое сообщение, аудио или видео файл")
    await state.set_state(UserState.waiting_for_transcribe)


@router.message(F.text == 'Сохраненные тексты')
async def show_texts(message: Message):
    texts = await db.get_texts(message.from_user.id, with_speakers=True)
    if not texts:
        await message.answer("Нет сохранённых текстов.")
        return
    
    await db.set_current_index(message.from_user.id, 0)
    await message.answer(
        f"Текст 1 из {len(texts)}:\n\n{texts[0]}",
        reply_markup=kb.text_nav
    )

@router.message(UserState.waiting_for_transcribe, F.voice | F.video | F.audio)
async def get_audio(message: Message, bot: Bot, state: FSMContext):
    try:
        if message.video:
            file = await bot.get_file(message.video.file_id)
            ext = ".mp4"
        elif message.voice:
            file = await bot.get_file(message.voice.file_id)
            ext = ".ogg"
        elif message.audio:
            file = await bot.get_file(message.audio.file_id)
            ext = ".mp3"
        else:
            await message.answer("Формат файла не поддерживается.")
            return

        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as temp_file:
            file_path = temp_file.name
            await bot.download_file(file.file_path, destination=file_path)

        if message.video:
            video = VideoFileClip(file_path)
            audio_path = file_path + ".wav"
            video.audio.write_audiofile(audio_path)
            video.close()
            os.unlink(file_path)
            file_path = audio_path

        if file_path.endswith('.ogg'):
            audio = AudioSegment.from_ogg(file_path)
            wav_path = file_path + ".wav"
            audio.export(wav_path, format="wav")
            os.unlink(file_path)
            file_path = wav_path

        await message.answer("Транскрибирую и диаризирую...")
        diarized_text, raw_text = transcribe_and_diarize(file_path)
        
        last_transcription[message.from_user.id] = diarized_text
        last_raw_transcription[message.from_user.id] = raw_text
        
        await message.answer(f"Результат с дикторами:\n\n{diarized_text}", 
                           reply_markup=kb.get_post_transcribe_keyboard())
        await state.clear()

    except Exception as e:
        await message.answer(f"Ошибка: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.unlink(file_path)
        wav_path = file_path + ".wav"
        if os.path.exists(wav_path):
            os.unlink(wav_path)

@router.callback_query(F.data == 'generate_tts')
async def request_voice_acting(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "Выбиретие способ озвучки текста",
        reply_markup=kb.voice_acting
    )

@router.callback_query(F.data == 'ready_voice')
async def request_ready_voice(callback: CallbackQuery, bot: Bot):
    current_index = await db.get_current_index(callback.from_user.id)
    texts = await db.get_texts(callback.from_user.id, with_speakers=False)
    
    if not texts or current_index >= len(texts):
        await callback.message.answer("Нет текста для озвучки.")
        return
    
    await callback.message.answer("Генерирую аудио...")

    output_path = f"tts_output_{callback.from_user.id}.mp3"

    tts.tts_to_file(
        text=texts[current_index],
        file_path=output_path,
        speaker="Ferran Simen",
        language="ru",
        split_sentences=True
    )
    with open(output_path, 'rb') as audio_file:
            audio_bytes = audio_file.read()
            input_file = BufferedInputFile(
            file=audio_bytes,
            filename="текст.mp3"
        )   
    await bot.send_audio(
        chat_id=callback.message.chat.id,
        audio=input_file,
    )

    if os.path.exists(output_path):
        os.unlink(output_path)

@router.callback_query(F.data == 'voice_sample')
async def request_voice_sample(callback: CallbackQuery, state: FSMContext):
    current_index = await db.get_current_index(callback.from_user.id)
    texts = await db.get_texts(callback.from_user.id, with_speakers=False)
    
    if not texts or current_index >= len(texts):
        await callback.message.answer("Нет текста для озвучки.")
        return
    
    last_text_for_tts[callback.from_user.id] = {
        'text': texts[current_index],
        'message_id': callback.message.message_id
    }
    
    await callback.message.answer("Пожалуйста, отправьте голосовое сообщение или аудиофайл как образец голоса.")
    await state.set_state(UserState.waiting_for_tts)


@router.message(UserState.waiting_for_tts, F.voice | F.audio)
async def handle_voice_sample(message: Message, bot: Bot, state: FSMContext):
    if message.from_user.id not in last_text_for_tts:
        return
    
    try:
        if message.voice:
            file = await bot.get_file(message.voice.file_id)
            ext = ".ogg"
        elif message.audio:
            file = await bot.get_file(message.audio.file_id)
            ext = ".mp3"
        
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as temp_file:
            voice_sample_path = temp_file.name
            await bot.download_file(file.file_path, destination=voice_sample_path)
        
        if ext == ".ogg":
            audio = AudioSegment.from_ogg(voice_sample_path)
            wav_path = voice_sample_path + ".wav"
            audio.export(wav_path, format="wav")
            os.unlink(voice_sample_path)
            voice_sample_path = wav_path
        
        text_data = last_text_for_tts[message.from_user.id]
        text_to_speak = text_data['text']
        await message.answer("Генерирую аудио...")
        output_path = f"tts_output_{message.from_user.id}.mp3"
        tts.tts_to_file(
            text=text_to_speak,
            file_path=output_path,
            speaker_wav=voice_sample_path,
            language="ru"
        )

        with open(output_path, 'rb') as audio_file:
            audio_bytes = audio_file.read()
            input_file = BufferedInputFile(
            file=audio_bytes,
            filename="текст.mp3"
        )   
        await bot.send_audio(
            chat_id=message.chat.id,
            audio=input_file,
        )
        
    except Exception as e:
        await message.answer(f"Ошибка при генерации аудио: {str(e)}")
    finally:
        del last_text_for_tts[message.from_user.id]
        if os.path.exists(voice_sample_path):
            os.unlink(voice_sample_path)
        if os.path.exists(output_path):
            os.unlink(output_path)
        await state.clear()

@router.callback_query(F.data == 'save_text')
async def save_text_callback(callback: CallbackQuery):
    diarized_text = last_transcription.get(callback.from_user.id)
    raw_text = last_raw_transcription.get(callback.from_user.id)
    if not diarized_text or not raw_text:
        await callback.message.answer("Нет текста для сохранения.")
        return
    
    await db.save_text(
        callback.from_user.id,
        diarized_text,
        raw_text,
        username=callback.from_user.username,
        full_name=callback.from_user.full_name
    )
    await callback.message.answer("Текст успешно сохранен")

@router.callback_query(F.data == 'prev_text')
async def prev_text(callback: CallbackQuery):
    texts = await db.get_texts(callback.from_user.id, with_speakers=True)
    if not texts:
        await callback.message.answer("Нет сохраненных текстов.")
        return
    
    current_index = await db.get_current_index(callback.from_user.id)
    new_index = max(0, current_index - 1)
    
    await db.set_current_index(callback.from_user.id, new_index)
    await callback.message.edit_text(
        f"Текст {new_index+1} из {len(texts)}:\n\n{texts[new_index]}",
        reply_markup=kb.text_nav
    )


@router.callback_query(F.data == 'next_text')
async def next_text(callback: CallbackQuery):
    texts = await db.get_texts(callback.from_user.id, with_speakers=True)
    if not texts:
        await callback.message.answer("Нет сохраненных текстов.")
        return
    
    current_index = await db.get_current_index(callback.from_user.id)
    new_index = min(len(texts) - 1, current_index + 1)
    
    await db.set_current_index(callback.from_user.id, new_index)
    await callback.message.edit_text(
        f"Текст {new_index+1} из {len(texts)}:\n\n{texts[new_index]}",
        reply_markup=kb.text_nav
    )

@router.callback_query(F.data == 'summarize_current')
async def summarize_current(callback: CallbackQuery):
    current_index = await db.get_current_index(callback.from_user.id)
    texts = await db.get_texts(callback.from_user.id, with_speakers=False)
    
    if not texts or current_index >= len(texts):
        await callback.message.answer("Нет текста для суммаризации.")
        return
    
    text = texts[current_index]
    await callback.message.answer("Суммаризирую...")

    try:
        input_length = len(text.split())
        max_len = int(input_length * 0.9)
        min_len = int(input_length * 0.6)

        summarizer = pipeline("summarization", model="IlyaGusev/rut5_base_sum_gazeta", device=0)
        result = summarizer(text, max_length=max_len, min_length=min_len)
        await callback.message.answer(f'Результат суммаризации:\n\n{result[0]["summary_text"]}')
    except Exception as e:
        await callback.message.answer(f"Ошибка при суммаризации: {str(e)}")

@router.callback_query(F.data == 'show_raw_text')
async def show_raw_text(callback: CallbackQuery):
    current_index = await db.get_current_index(callback.from_user.id)
    texts = await db.get_texts(callback.from_user.id, with_speakers=False)
    
    if not texts or current_index >= len(texts):
        await callback.message.answer("Нет сохраненных текстов.")
        return
    
    await callback.message.answer(
        f"Текст {current_index+1} без дикторов:\n\n{texts[current_index]}"
    )