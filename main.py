import time
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor

import yt_dlp
from aiogram import Bot, Dispatcher, Router, types
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import FSInputFile
import aiohttp
from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Получаем путь к файлу базы данных
db_path = os.path.join(os.getcwd(), 'users.db')
engine = create_engine(f'sqlite:///{db_path}', echo=True)

# Создание базового класса (Base)
Base = declarative_base()
youtube_dowload = {}
# Определение таблицы с использованием класса
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    download_format = Column(String(50))
    solution = Column(String(50))

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


ydlp_formats = {'bestvideo+bestaudio/best': 'Лучшее качество',
                'bestvideo[height<=720]+bestaudio/best[height<=720]': 'Среднее качество',
                'worstvideo+worstaudio/worst': 'Худшее качество'
                }


bot = Bot(token='')
dp = Dispatcher()
router = Router()
executor = ThreadPoolExecutor(max_workers=10)  # Настройте max_workers в зависимости от ваших требований


class Work(StatesGroup):
    download = State()
    change_format = State()
    change_solution = State()


def download_video_register(url, quality='best', preferred_format='mp4'):
    import uuid
    id_video = uuid.uuid4()
    path = f'C:/Users/Gdjsb/PycharmProjects/pythonProject/{id_video}.{preferred_format}'
    ydl_opts = {
        'format': quality,
        'postprocessors': [{
            'key': 'FFmpegVideoConvertor',
            'preferedformat': preferred_format
        }],
        'outtmpl': f'C:/Users/Gdjsb/PycharmProjects/pythonProject/{id_video}.%(ext)s',
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(url, download=True)
            file_size = os.path.getsize(path) / (1024 * 1024)
            return path, file_size
    except Exception as e:
        print(f'Ошибка скачивания видео: {e}')
        return None

async def list_formats(ytlink):
    ydl_opts = {}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(ytlink, download=False)
        formats = info_dict.get('formats', [])
        filtered_formats = [
            f for f in formats
            if f.get('height') is not None and f.get('filesize') is not None
        ]

    formats_youtube = {}
    builder = InlineKeyboardBuilder()
    builder.adjust(1)

    for f in filtered_formats:
        filesize = f.get('filesize', 'Неизвестно')
        height = f.get('height', 'Неизвестно')
        format_id = f.get('format_id')
        container = f.get('ext', 'Неизвестно')
        filesize_mb = filesize / (1024 * 1024) if filesize != 'Unknown' else 'Unknown'
        formats_youtube[format_id] = {'Resolution': height, 'Filesize': f'{filesize_mb:.2f}', 'Format': container}
        builder.button(text=format_id, callback_data=str(format_id))

    return formats_youtube, builder


@router.message()
async def start_message(message: types.Message, state: FSMContext):
    print(message.from_user.id)
    await state.clear()
    global session
    if message.text.startswith('https://'):
        await message.answer('Приняли вашу ссылку в работу.')
        ytlink = message.text
        user_data = session.query(User).filter_by(id=message.from_user.id).first()

        if user_data is None:
            formats_youtube, builder = await list_formats(ytlink)
            # Если пользователь не зарегистрирован, запрашиваем данные
            mess = ''
            for format_id, details in formats_youtube.items():
                mess += f'\nID: {format_id} Разрешение: {details["Resolution"]}p. Размер: {details["Filesize"]}MB Формат: {details["Format"]}'

            mess += '\nВидео длиннее 20 мб будет отправленно в виде ссылки на скачивание!!!! (Лимит 2GB):'
            builder.adjust(3)
            youtube_dowload[message.from_user.id] = formats_youtube
            await message.answer(mess, reply_markup=builder.as_markup())
            await state.set_state(Work.download)
            await state.update_data(url=message.text)
            await state.update_data(ytbdl_data=formats_youtube)
            return
        else:
            # Если пользователь зарегистрирован, используем данные из БД
            loop = asyncio.get_event_loop()
            filename, filesize = await loop.run_in_executor(executor, download_video_register, ytlink,
                                                            user_data.solution, user_data.download_format)
            await message.answer('Видео успешно скачено!')
            if float(filesize) > 20:
                err = 0
                while err < 2:
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.post('https://file.io',
                                                    data={'file': open(filename, 'rb')}) as response:
                                response.raise_for_status()
                                json_data = await response.json()
                                await message.answer('Готово! ' + str(json_data['link']))
                                os.remove(filename)
                                return
                    except aiohttp.ClientError as e:
                        print(f'Ошибка загрузки файла: {e} Пытаемся еще раз.')
                        await message.answer('Ошибка загрузки файла. Пытаемся еще раз.')
                        err += 1
            else:
                try:
                    await message.answer('Отправляем...')
                    err = 0
                    while err < 2:
                        try:
                            video = FSInputFile(filename)
                            await message.answer_video(video, caption="Спасибо за использование нашего бота!)")
                            os.remove(filename)
                            return
                        except:
                            err += 1
                except Exception as e:
                    print(f'Ошибка отправки видео: {e}')
                    await message.answer('Ошибка отправки видео.')
            if not filename:
                await message.answer('Ошибка скачивания видео.')
                return
    else:
        builder = InlineKeyboardBuilder()
        builder.button(text='Настройки', callback_data='settings')
        await message.answer('Здравствуйте! Для скачивания видео с ютуб пришлите ссылку на видео!',
                             reply_markup=builder.as_markup())


@router.callback_query(lambda callback_query: callback_query.data == 'settings')
async def settings(callback_query: types.CallbackQuery, state: FSMContext):
    global ydlp_formats
    user_data = session.query(User).filter_by(id=callback_query.from_user.id).first()
    print(callback_query.from_user.id)
    try:
        if user_data.id != None:
            pass
    except AttributeError:
        new_user = User(id=callback_query.from_user.id, download_format='mp4', solution='bestvideo+bestaudio/best')
        session.add(new_user)
        session.commit()
        user_data = session.query(User).filter_by(id=callback_query.from_user.id).first()
    message = f'Ваш ID: {user_data.id}, Формат файла: {user_data.download_format} Качество: {ydlp_formats[user_data.solution]}'
    builder = InlineKeyboardBuilder()
    builder.button(text='Изменить фомрат', callback_data='change_download_format')
    builder.button(text='Изменить качество', callback_data='change_solution')
    builder.button(text='Удалить профиль ', callback_data='delete')
    await callback_query.message.answer(message, reply_markup=builder.as_markup())


@router.callback_query(lambda callback_query: callback_query.data == 'delete')
async def delete(callback_query: types.CallbackQuery, state: FSMContext):
    user_data = session.query(User).filter_by(id=callback_query.from_user.id).first()
    session.delete(user_data)
    session.commit()
    await callback_query.message.answer('Успешно!')


@router.callback_query(lambda callback_query: callback_query.data == 'change_solution')
async def change_solution(callback_query: types.CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text='Лучшее качество', callback_data='bestvideo+bestaudio/best')
    builder.button(text='Среднее качество', callback_data='bestvideo[height<=720]+bestaudio/best[height<=720]')
    builder.button(text='Худшее качество', callback_data='worstvideo+worstaudio/worst')
    builder.button(text='Назад', callback_data='settings')
    await callback_query.message.answer('Выберите формат:', reply_markup=builder.as_markup())
    await state.set_state(Work.change_solution)


@router.callback_query(Work.change_solution)
async def change_solution_conf(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.data in ['bestvideo+bestaudio/best', 'bestvideo[height<=720]+bestaudio/best[height<=720]', 'worstvideo+worstaudio/worst']:
        user_data = session.query(User).filter_by(id=callback_query.from_user.id).first()
        user_data.solution = callback_query.data
        session.commit()
        await callback_query.message.answer('Готово')
    else:
        await callback_query.message.answer('Я вас не понял.')
    await state.clear()


@router.callback_query(lambda callback_query: callback_query.data == 'change_download_format')
async def change_download_format(callback_query: types.CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.button(text='mp4', callback_data='mp4')
    builder.button(text='webm', callback_data='webm')
    builder.button(text='Назад', callback_data='settings')
    await callback_query.message.answer('Выберите формат:', reply_markup=builder.as_markup())
    await state.set_state(Work.change_format)


@router.callback_query(Work.change_format)
async def change_format(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.data in ['mp4', 'webm']:
        user_data = session.query(User).filter_by(id=callback_query.from_user.id).first()
        user_data.download_format = callback_query.data
        session.commit()
        await callback_query.message.answer('Готово')
    else:
        await callback_query.message.answer('Я вас не понял.')
    await state.clear()


@router.callback_query(Work.download)
async def download(callback_query: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    url = data['url']
    format_id = callback_query.data
    yt_data = data['ytbdl_data'][format_id]
    await state.clear()
    await callback_query.message.answer('Скачиваем... Не создавайте новые запросы на скачивание видео!')
    loop = asyncio.get_event_loop()
    filename = await loop.run_in_executor(executor, download_video, url, format_id)
    if not filename:
        await callback_query.message.answer('Ошибка скачивания видео.')
        return
    await callback_query.message.answer('Видео успешно скачено!')
    if float(yt_data["Filesize"]) > 20:
        await upload_and_send_link(callback_query, filename)
    else:
        try:
            await callback_query.message.answer('Отправляем...')
            video = FSInputFile(filename)
            await callback_query.message.answer_video(video, caption="Спасибо за использование нашего бота!)")
        except Exception as e:
            print(f'Ошибка отправки видео: {e}')
            await callback_query.message.answer('Ошибка отправки видео.')

    await asyncio.sleep(10)
    os.remove(filename)


def download_video(url, format_id):
    import uuid
    ydl_opts = {
        'format': f'{format_id}+bestaudio',
        'outtmpl': f'C:/Users/Gdjsb/PycharmProjects/pythonProject/{uuid.uuid4()}.%(ext)s',
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(url, download=True)
        time.sleep(1)
        return ydl.prepare_filename(info_dict)
    except Exception as e:
        print(f'Ошибка скачивания видео: {e}')
        return None


async def upload_and_send_link(callback_query, filename):
    await callback_query.message.answer('Видео загружается в облако...')
    err = 0
    while err < 2:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post('https://file.io', data={'file': open(filename, 'rb')}) as response:
                    response.raise_for_status()
                    json_data = await response.json()
                    await callback_query.message.answer('Готово! ' + str(json_data['link']))
                    return
        except aiohttp.ClientError as e:
            print(f'Ошибка загрузки файла: {e} Пытаемся еще раз.')
            await callback_query.message.answer('Ошибка загрузки файла. Пытаемся еще раз.')
            err += 1
    await callback_query.message.answer('Ошибка загрузки файла. Попробуйте позже...')


async def main():
    start_data = await bot.get_me()
    print(f'https://t.me/{start_data.username}')
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
