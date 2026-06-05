import re
from typing import List

from pydantic import BaseModel


class GlobusCredentials(BaseModel):
    client_id: str
    client_secret: str


# Convert a text field into a list of strings
def textfield_to_strlist(textfield: str) -> List[str]:
    try:
        str_list = re.split(r"[\s;]+", textfield.strip())
        return [s for s in str_list if s]
    except Exception as e:
        raise ValueError("Could not convert textfield into a list of strings.") from e
