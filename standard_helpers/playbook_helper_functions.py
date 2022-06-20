class PlaybookHelperFunctions:
    """
    Helper functions used within the playbooks
    """

    def remove_from_supply_chain(supply_chain, ids):
        """
        Removes the ID at the end of the inputted list of ids from the supply_chain tree

        :param supply_chain: A multi-dimensional dictionary representing a supply chain
        :param ids: A list of IDs starting from the root to leaf of the path to the ID you would like to remove
        """
        if len(ids) > 2:
            remove_from_supply_chain(supply_chain[ids[0]], ids[1:])
        else:
            supply_chain[ids[0]].pop(ids[1])
            if len(supply_chain[ids[0]]) == 0:
                supply_chain.pop(ids[0])
