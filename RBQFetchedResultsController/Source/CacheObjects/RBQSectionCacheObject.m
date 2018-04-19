//
//  RBQSectionCacheObject.m
//  RBQFetchedResultsControllerExample
//
//  Created by Adam Fish on 1/6/15.
//  Copyright (c) 2015 Roobiq. All rights reserved.
//

#import "RBQSectionCacheObject.h"
#import "RBQFetchRequest.h"

@implementation RLMSortDescriptor (RBQCategory)

- (void)encodeWithCoder:(NSCoder *)aCoder
{
    [aCoder encodeObject:self.keyPath forKey:@"keyPath"];
    [aCoder encodeBool:self.ascending forKey:@"ascending"];
}

- (nullable instancetype)initWithCoder:(NSCoder *)aDecoder
{
    NSString *keyPath = [aDecoder decodeObjectForKey:@"keyPath"];
    BOOL ascending = [aDecoder decodeBoolForKey:@"ascending"];
    
    self = [RLMSortDescriptor sortDescriptorWithKeyPath:keyPath ascending:ascending];
    return self;
}

@end

@implementation RBQSectionCacheObject

+ (NSString *)primaryKey
{
    return @"name";
}

+ (NSDictionary *)defaultPropertyValues
{
    return @{@"name" : @""
             };
}

#pragma mark - Equality

- (BOOL)isEqualToObject:(RBQSectionCacheObject *)object
{
    BOOL hasEqualPredicate = (!self.predicateData && !object.predicateData) || [self.predicateData isEqual:object.predicateData];
    BOOL hasEqualDistinct = (!self.distinctByData && !object.distinctByData) || [self.distinctByData isEqual:object.distinctByData];
    BOOL hasEqualSort = (!self.sortDescriptorsData && !object.sortDescriptorsData) || [self.sortDescriptorsData isEqual:object.sortDescriptorsData];
    return [self.name isEqualToString:object.name] && hasEqualPredicate && hasEqualDistinct && hasEqualSort;
}

- (BOOL)isEqual:(id)object
{
    if (self == object) {
        return YES;
    }
    
    if (![object isKindOfClass:[RBQSectionCacheObject class]]) {
        return NO;
    }
    
    return [self isEqualToObject:object];
}

#pragma mark - Public Methods

+ (instancetype)cacheWithName:(NSString *)name
{
    RBQSectionCacheObject *section = [[RBQSectionCacheObject alloc] init];
    section.name = name;
    
    return section;
}

@end
