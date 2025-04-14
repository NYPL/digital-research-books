import PIL


def resize_image_for_cover(image: PIL.Image) -> PIL.Image:

    original_width = image.width
    original_height = image.height
    original_ratio = image.width / image.height

    resize_height = 400
    resize_width = 300

    if 400 * original_ratio > 300 and original_ratio > 1:
        resize_height = int(round(300 / original_ratio))
    elif 400 * original_ratio > 300:
        resize_height = int(round(300 * original_ratio))
    else:
        resize_width = int(round(400 * original_ratio))

    return image.resize((resize_width, resize_height))
