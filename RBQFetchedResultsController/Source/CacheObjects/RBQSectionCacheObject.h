//
//  RBQSectionCacheObject.h
//  RBQFetchedResultsControllerExample
//
//  Created by Adam Fish on 1/6/15.
//  Copyright (c) 2015 Roobiq. All rights reserved.
//

#import <Realm/Realm.h>

@interface RLMSortDescriptor (RBQCategory) <NSCoding>
@end

/**
 * Internal object used by RBQFetchedResultsController cache. Object represents a section within the FRC cache.
 *
 * @warning This class is not to be used external the RBQFetchedResultsController
 */
@interface RBQSectionCacheObject : RLMObject

/**
 *  Section name
 */
@property NSString *name;

/**
 *  Original RLMObject class name
 */
@property NSString *className;

/**
 *  Data to reproduce section query
 */
@property NSData *predicateData;
@property NSData *sortDescriptorsData;
@property NSData *distinctByData;

/**
 *  Create RBQSectionCacheObject with a given section name
 *
 *  @param name The name of the section
 *
 *  @return A new instance of RBQSectionCacheObject
 */
+ (instancetype)cacheWithName:(NSString *)name;

@end

// This protocol enables typed collections. i.e.:
// RLMArray<RBQSectionCacheObject>
