/**
 * Takes input from toml file named {configPath}/schema/graphDependencies.js and folder {configPath}/schema/entities. Generates ES schema for all the entity types in the graph.
 *
 * Based on the copyTo and unionIn dependencies on the graph,
 * an entity will have nested documents containing joined
 * information of potentially infinite depth, from other related tables. So this generates nested mapping for storing data from joined relationships. 
 * 
 */

const generate = (configPath)
