import re
import unicodedata

def remove_accents(text):
    """
    Normalize Vietnamese by removing all diacritics (accents).
    Example: 'Tiếng Việt có dấu' -> 'Tieng Viet co dau'
    """
    # Normalize unicode (NFD decomposes accents)
    text = unicodedata.normalize('NFD', text)
    
    # Remove combining diacritical marks
    text = re.sub(r'[\u0300-\u036f]', '', text)
    
    # Replace special Vietnamese characters manually
    text = text.replace('đ', 'd').replace('Đ', 'D')
    
    return text

def normalize_vietnamese(text):
    """
    Normalize Vietnamese text by removing accents and converting to lowercase.
    """
    if not isinstance(text, str):
        return text
    # Remove accents & convert to lowercase
    text = remove_accents(text).lower()
    
    # Remove special characters
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove leading/trailing spaces
    text = text.strip()
    
    return text

def rm_cau(text):
    return re.sub(r'cau \d+', '', text).strip()

def check_oa_in_q(row):
    try:
        if row["oa"] in row["q"]:
            return True
        else:
            return False
    except:
        return False
