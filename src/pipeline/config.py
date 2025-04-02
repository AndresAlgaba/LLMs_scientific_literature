EMAIL = "..."
APPLICATION_NAME = "..."

SYSTEM_PROMPT = """
Below, we share with you the title, authors, year, venue, and abstract of a scientific paper.
 Can you provide {n} references that would be relevant to this paper?
""".strip()

#  It is important that you do not hallucinate any references.
#  It is better to provide references that are only tangentially related to the paper than to
#  provide references that do not exist.
#  Make sure that you return {n} references.

PROCESS_PROMPT = """
Below, we share with you a list of references.
 Could you for each reference extract the authors, the number of authors,
 title, publication year, and publication venue?
 Please only return the extracted information in a markdown table with the
 authors, number of authors, title, publication year, and publication venue as columns.
 Do not return any additional information or formatting, such as ```markdown:
| Authors | Number of Authors | Title | Publication Year | Publication Venue |
 Make sure to respect this format.
""".strip()
