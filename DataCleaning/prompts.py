gemini_prompt="""
# You are an AI specialized in generating structured medical multiple-choice questions. Your task is to process a list of questions and return a list of JSON objects.
# Translate question and answer to Vietnamese language, clean any special characters, and remove any extra spaces, clean any html tags, ...
# Skip question that violate policy, can not be processed
# Each JSON object should include:
# - question: Question text
# - answers: {optionA, optionB, optionC, optionD}
# - correctAnswer: Correct answer option (optionA to optionD)
# - medicalTopic: List of relevant medical topics (categories). If category does not fit in medical domain, fill Other(No Category)
# - difficultLevel: Difficulty level
# - regularFormat: default True
# - questionID: Unique question ID

# ### Below are the difficulty levels:
# Level 1 (Easy): Questions that are factual or definitional. Example: Basic definitions.
# Level 2 (Moderate): Questions requiring understanding of mechanisms or drug properties. Example: Mechanism of diseases or drugs.
# Level 3 (Challenging): Patient-centered scenarios requiring clinical reasoning. Example: Clinical case study with diagnostic steps.
# Level 4 (Hard): Complex case studies requiring advanced reasoning. Example: Multi-faceted patient scenarios involving critical thinking.

# ### Medical Categories:
# Allergy and Immunology
# Anesthesiology
# Cardiology
# Dermatology
# Endocrinology
# Gastroenterology
# Geriatrics
# Hematology
# Infectious Diseases
# Internal Medicine
# Nephrology
# Neurology
# Nuclear Medicine
# Obstetrics and Gynecology
# Oncology
# Ophthalmology
# Orthopedics
# Otolaryngology
# Palliative Medicine
# Pathology
# Pediatrics
# Physical Medicine and Rehabilitation
# Psychiatry
# Pulmonology
# Radiology
# Rheumatology
# Sports Medicine
# Surgery
# Urology
# General Medicine
# Eastern Medicine
# Public Health
# Preventive Healthcare
# Emergency Medicine
# Other(No Category)

# ### Expected JSON Structure:
# {
#   "question": "A well-formed medical question related to healthcare.",
#   "answers": {
#     "optionA": "A possible answer choice.",
#     "optionB": "A possible answer choice.",
#     "optionC": "A possible answer choice.",
#     "optionD": "A possible answer choice."
#   },
#   "correctAnswer": "The correct answer among the options (e.g., 'optionA').",
#   "medicalTopic": ["Relevant medical categories (e.g., Cardiology, Neurology)."],
#   "difficultLevel": "The difficulty level (Easy, Medium, Hard).",
#   "regularFormat": true,
#   "questionID": "A unique identifier for the question."
# }

# Now, process the following list of questions and return the output in JSON format (one JSON object per question):

# **Return only JSON objects in a list. No extra explanations. No additional text.**
# """