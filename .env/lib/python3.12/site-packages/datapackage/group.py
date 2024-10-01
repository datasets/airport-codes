from itertools import chain


# Module API

class Group(object):
    """Group representation

    # Arguments
        Resource[]: list of TABULAR resources

    """

    # Public

    def __init__(self, resources):

        # Contract checks
        assert resources
        assert all([resource.tabular for resource in resources])
        assert all([resource.group for resource in resources])

        # Get props from the resources
        self.__name = resources[0].group
        self.__headers = resources[0].headers
        self.__schema = resources[0].schema
        self.__resources = resources

    @property
    def name(self):
        """Group name

        # Returns
            str: name

        """
        return self.__name

    @property
    def headers(self):
        """Group's headers

        # Returns
            str[]/None: returns headers

        """
        return self.__headers

    @property
    def schema(self):
        """Resource's schema

        # Returns
            tableschema.Schema: schema

        """
        return self.__schema

    def iter(self, **options):
        """Iterates through the group data and emits rows cast based on table schema.

        > It concatenates all the resources and has the same API as `resource.iter`

        """
        return chain(*[resource.iter(**options) for resource in self.__resources])

    def read(self, limit=None, **options):
        """Read the whole group and return as array of rows

        > It concatenates all the resources and has the same API as `resource.read`

        """
        rows = []
        for count, row in enumerate(self.iter(**options), start=1):
            rows.append(row)
            if count == limit:
                break
        return rows

    def check_relations(self):
        """Check group's relations

        The same as `resource.check_relations` but without the optional
        argument *foreign_keys_values*.  This method will test foreignKeys of the
        whole group at once otpimizing the process by creating the foreign_key_values
        hashmap only once before testing the set of resources.

        """
        # opti relations should ne loaded only once for the group
        foreign_keys_values = self.__resources[0].get_foreign_keys_values()

        # alternative to check_relations from tableschema-py
        for resource in self.__resources:
            resource.check_relations(foreign_keys_values=foreign_keys_values)
        return True
