import re

class Constant:
    """ Container class for meant for grouped constants """

    @classmethod
    def all(cls):
        """
        Returns all constant values in class with the attribute requirements:
          - only uppercase letters and underscores
          - must begin and end with a letter

        """
        regex = r'^[A-Z][A-Z_]*[A-Z]$'
        return [kv[1] for kv in cls.__dict__.items() if re.match(regex, kv[0])]

class CONSTS(Constant):
  # Make sure that the access to this folder is permitted
  LOCAL_PARQUETS_DESTINATION_FOLDER = "C:/Users/volo348417/OneDrive - Exact Group B.V/Projects/ds-transformations/pyspark_playground/parquets_domain_base/"