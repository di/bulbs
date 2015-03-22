# -*- coding: utf-8 -*-
#
# Copyright 2012 James Thornton (http://jamesthornton.com)
# BSD License (see LICENSE for details)
#
"""
Interface for interacting with a graph database through OrientDB.

"""
import os
import io
from bulbs.config import Config
from bulbs.gremlin import Gremlin
from bulbs.element import Vertex, Edge
from bulbs.model import Node, Relationship
from bulbs.base.graph import Graph as BaseGraph

# OrientDB-specific imports
from .client import OrientDBClient
from .index import ManualIndex


class Graph(BaseGraph):

    """
    The primary interface to OrientDB.

    Instantiates the database :class:`~bulbs.orientdb.client.Client` object
    using the specified Config and sets up proxy objects to the database.

    :param config: Optional. Defaults to the default config.
    :type config: bulbs.config.Config

    :cvar client_class: OrientDBClient class.
    :cvar default_index: Default index class.

    :ivar client: OrientDBClient object.
    :ivar vertices: VertexProxy object.
    :ivar edges: EdgeProxy object.
    :ivar config: Config object.
    :ivar gremlin: Gremlin object.
    :ivar scripts: GroovyScripts object.

    Example:

    >>> from bulbs.orientdb import Graph
    >>> g = Graph()
    >>> james = g.vertices.create(name="James")
    >>> julie = g.vertices.create(name="Julie")
    >>> g.edges.create(james, "knows", julie)

    """
    client_class = OrientDBClient
    default_index = ManualIndex

    def __init__(self, config=None):
        super(Graph, self).__init__(config)

        # OrientDB supports Gremlin
        self.gremlin = Gremlin(self.client)
        self.scripts = self.client.scripts    # for convienience

    def make_script_files(self, out_dir=None):
        """
        Generates a server-side scripts file.

        """
        out_dir = out_dir or os.getcwd()
        for namespace in self.scripts.namespace_map:
            # building script content from stored methods
            # instead of sourcing files directly to filter out overridden
            # methods
            methods = self.scripts.namespace_map[namespace]
            scripts_file = os.path.join(out_dir, "%s.groovy" % namespace)
            method_defs = []
            for method_name in methods:
                method = methods[method_name]
                method_defs.append(method.definition)
            content = "\n\n".join(method_defs)
            with io.open(scripts_file, "w", encoding='utf-8') as fout:
                fout.write(content + "\n")

    def load_graphml(self, uri):
        """
        Loads a GraphML file into the database and returns the response.

        :param uri: URI of the GraphML file to load.
        :type uri: str

        :rtype: OrientDBResult

        """
        script = self.client.scripts.get('load_graphml')
        params = dict(uri=uri)
        return self.gremlin.command(script, params)

    def get_graphml(self):
        """
        Returns a GraphML file representing the entire database.

        :rtype: OrientDBResult

        """
        script = self.client.scripts.get('save_graphml')
        return self.gremlin.command(script, params=None)

    def warm_cache(self):
        """
        Warms the server cache by loading elements into memory.

        :rtype: OrientDBResult

        """
        script = self.scripts.get('warm_cache')
        return self.gremlin.command(script, params=None)

    def clear(self):
        """
        Deletes all the elements in the graph.

        :rtype: OrientDBResult

        .. admonition:: WARNING

           This will delete all your data!

        """
        script = self.client.scripts.get('clear')
        return self.gremlin.command(script, params=None)
