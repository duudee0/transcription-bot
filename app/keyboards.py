from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

main = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='Транскрибация и диаризация')],
    [KeyboardButton(text='Сохраненные тексты')]
],
resize_keyboard=True,
input_field_placeholder='Выберите пункт')

voice_acting = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="Готовый голос", callback_data='ready_voice')],
    [InlineKeyboardButton(text="Образец голоса", callback_data='voice_sample')]
])

text_nav = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='⬅️', callback_data='prev_text'),
     InlineKeyboardButton(text='➡️', callback_data='next_text')],
    [InlineKeyboardButton(text='Суммаризация', callback_data='summarize_current'),
     InlineKeyboardButton(text='Исходный текст', callback_data='show_raw_text')],
    [InlineKeyboardButton(text='Озвучить текст', callback_data='generate_tts')]
])

def get_post_transcribe_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сохранить текст", callback_data="save_text")]
    ])
