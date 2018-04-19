//
//  RBQFetchedResultsController.m
//  RBQFetchedResultsControllerTest
//
//  Created by Adam Fish on 1/2/15.
//  Copyright (c) 2015 Roobiq. All rights reserved.
//

#import "RBQFetchedResultsController.h"

#import "RBQControllerCacheObject.h"
#import "RBQSectionCacheObject.h"

#import <objc/runtime.h>
#import "pthread.h"
#import <RealmUtilities/RLMObject+Utilities.h>

@import UIKit;

#pragma mark - Constants
static void * RBQArrayFetchRequestContext = &RBQArrayFetchRequestContext;

#pragma mark - RBQFetchedResultsController

@interface RBQFetchedResultsController ()

@property (nonatomic, strong) RLMNotificationToken *notificationToken;
@property (nonatomic, strong) id<RLMCollection> notificationCollection;
@property (nonatomic, strong) NSRunLoop *notificationRunLoop;

@property (strong, nonatomic) RLMRealm *inMemoryRealm;
@property (strong, nonatomic) RLMRealm *realmForMainThread; // Improves scroll performance
@property (strong, nonatomic) NSMutableDictionary <NSString *, NSMutableDictionary<NSString *, RLMResults *> *> *sectionObjectsPerThread;

@end

#pragma mark - RBQFetchedResultsSectionInfo

@interface RBQFetchedResultsSectionInfo ()

// RBQFetchRequest to support retrieving section objects
@property (strong, nonatomic) RBQFetchRequest *fetchRequest;

// Section name key path to support retrieving section objects
@property (strong, nonatomic) NSString *sectionNameKeyPath;

// Create a RBQFetchedResultsSectionInfo
+ (instancetype)createSectionWithName:(NSString *)sectionName
                   sectionNameKeyPath:(NSString *)sectionNameKeyPath
                         fetchRequest:(RBQFetchRequest *)fetchRequest;

@end

@implementation RBQFetchedResultsSectionInfo
@synthesize name = _name;

+ (instancetype)createSectionWithName:(NSString *)sectionName
                   sectionNameKeyPath:(NSString *)sectionNameKeyPath
                         fetchRequest:(RBQFetchRequest *)fetchRequest
{
    RBQFetchedResultsSectionInfo *sectionInfo = [[RBQFetchedResultsSectionInfo alloc] init];
    sectionInfo->_name = sectionName;
    sectionInfo.sectionNameKeyPath = sectionNameKeyPath;
    sectionInfo.fetchRequest = fetchRequest;
    
    return sectionInfo;
}

- (id<RLMCollection>)objects
{
    if (self.fetchRequest && self.sectionNameKeyPath) {
        
        id<RLMCollection> fetchResults = [self.fetchRequest fetchObjects];
        
        return [fetchResults objectsWhere:@"%K == %@", self.sectionNameKeyPath, self.name];
    }
    else if (self.fetchRequest) {
        return [self.fetchRequest fetchObjects];
    }
    
    return nil;
}

- (NSUInteger)numberOfObjects
{
    return [self objects].count;
}

@end

#pragma mark - RBQFetchedResultsController

@implementation RBQFetchedResultsController
@synthesize cacheName = _cacheName;

#pragma mark - Public Class

+ (void)deleteCacheWithName:(NSString *)name
{
    if (name) {
        RLMRealm *cacheRealm = [RBQFetchedResultsController realmForCacheName:name];
        
        [cacheRealm beginWriteTransaction];
        [cacheRealm deleteAllObjects];
        [cacheRealm commitWriteTransaction];
    }
    // No name, so lets clear all caches
    else {
        NSError *error;
        if (![[NSFileManager defaultManager] removeItemAtPath:[RBQFetchedResultsController basePathForCaches]
                                                        error:&error]) {
#ifdef DEBUG
            NSLog(@"%@",error.localizedDescription);
#endif
        }
    }
}

+ (NSArray *)allCacheRealmPaths
{
    NSString *basePath = [RBQFetchedResultsController basePathForCaches];
    
    NSURL *baseURL = [[NSURL alloc] initFileURLWithPath:basePath isDirectory:YES];
    
    NSError *error = nil;
    NSArray *urlsInSyncCache =
    [[NSFileManager defaultManager] contentsOfDirectoryAtURL:baseURL
                                  includingPropertiesForKeys:@[NSURLIsDirectoryKey, NSURLNameKey]
                                                     options:0
                                                       error:&error];
    
    if (error) {
        NSLog(@"Error retrieving sync cache directories: %@", error.localizedDescription);
        
    }
    
    NSMutableArray *cachePaths = [NSMutableArray array];
    
    for (NSURL *url in urlsInSyncCache) {
        NSNumber *isDirectory = nil;
        NSError *error = nil;
        
        if (![url getResourceValue:&isDirectory
                            forKey:NSURLIsDirectoryKey
                             error:&error]) {
            
            NSLog(@"Error retrieving resource value: %@", error.localizedDescription);
        }
        
        if (isDirectory.boolValue) {
            NSString *name = nil;
            
            if (![url getResourceValue:&name
                                forKey:NSURLNameKey
                                 error:&error]) {
                
                NSLog(@"Error retrieving resource value: %@", error.localizedDescription);
            }
            else {
                // Directory name is filename with extension stripped
                NSString *cachePath = [RBQFetchedResultsController cachePathWithName:name];
                
                [cachePaths addObject:cachePath];
            }
        }
    }
    
    return cachePaths.copy;
}

#pragma mark - Private Class

// Create Realm instance for cache name
+ (RLMRealm *)realmForCacheName:(NSString *)cacheName
{
    NSURL *url = [NSURL fileURLWithPath:[RBQFetchedResultsController cachePathWithName:cacheName]];
    
    RLMRealmConfiguration *config = [RLMRealmConfiguration defaultConfiguration];
    config.fileURL = url;
    config.encryptionKey = nil;
    config.objectClasses = @[RBQControllerCacheObject.class, RBQSectionCacheObject.class];
    
    return [RLMRealm realmWithConfiguration:config error:nil];;
}

//  Create a file path for Realm cache with a given name
+ (NSString *)cachePathWithName:(NSString *)name
{
    NSString *basePath = [RBQFetchedResultsController basePathForCaches];
    
    BOOL isDir = NO;
    NSError *error = nil;
    
    //Create a unique directory for each cache
    NSString *uniqueDirectory = [NSString stringWithFormat:@"/%@/",name];
    
    NSString *cachePath = [basePath stringByAppendingPathComponent:uniqueDirectory];
    
    if (![[NSFileManager defaultManager] fileExistsAtPath:cachePath isDirectory:&isDir] && isDir == NO) {
        [[NSFileManager defaultManager] createDirectoryAtPath:cachePath
                                  withIntermediateDirectories:NO
                                                   attributes:@{NSFileProtectionKey:NSFileProtectionNone}
                                                        error:&error];
        
        if (error) {
#ifdef DEBUG
            NSLog(@"FRC Cache Directory Creation Error: %@",error.localizedDescription);
#endif
        }
    }
    
    NSString *fileName = [NSString stringWithFormat:@"%@.realm",name];
    
    cachePath = [cachePath stringByAppendingPathComponent:fileName];
    
    return cachePath;
}

+ (NSString *)basePathForCaches
{
    NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
    NSString *documentPath = [paths objectAtIndex:0];
    BOOL isDir = NO;
    NSError *error = nil;
    
    //Base path for all caches
    NSString *basePath = [documentPath stringByAppendingPathComponent:@"/RBQFetchedResultsControllerCache"];
    
    if (![[NSFileManager defaultManager] fileExistsAtPath:basePath isDirectory:&isDir] && isDir == NO) {
        [[NSFileManager defaultManager] createDirectoryAtPath:basePath
                                  withIntermediateDirectories:NO
                                                   attributes:@{NSFileProtectionKey:NSFileProtectionNone}
                                                        error:&error];
        
        if (error) {
#ifdef DEBUG
            NSLog(@"FRC Cache Directory Creation Error: %@",error.localizedDescription);
#endif
        }
    }
    
    return basePath;
}

+ (NSString *)basePathForCacheWithName:(NSString *)name
{
    NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
    NSString *documentPath = [paths objectAtIndex:0];
    BOOL isDir = NO;
    NSError *error = nil;
    
    //Unique directory for the cache
    NSString *uniqueDirectory = [NSString stringWithFormat:@"/RBQFetchedResultsControllerCache/%@",name];
    
    NSString *cachePath = [documentPath stringByAppendingPathComponent:uniqueDirectory];
    
    if (![[NSFileManager defaultManager] fileExistsAtPath:cachePath isDirectory:&isDir] && isDir == NO) {
        [[NSFileManager defaultManager] createDirectoryAtPath:cachePath
                                  withIntermediateDirectories:NO
                                                   attributes:@{NSFileProtectionKey:NSFileProtectionNone}
                                                        error:&error];
        
        if (error) {
#ifdef DEBUG
            NSLog(@"FRC Cache Directory Creation Error: %@",error.localizedDescription);
#endif
        }
    }
    
    return cachePath;
}

#pragma mark - Public Instance

- (void)dealloc
{
    // Remove the notifications
    [self unregisterChangeNotifications];
}

- (id)initWithFetchRequest:(RBQFetchRequest *)fetchRequest
        sectionNameKeyPath:(NSString *)sectionNameKeyPath
                 cacheName:(NSString *)name
{
    self = [super init];
    
    if (self) {
        _cacheName = name;
        _fetchRequest = fetchRequest;
        _sectionNameKeyPath = sectionNameKeyPath;
        _sectionObjectsPerThread = [@{} mutableCopy];
        
#ifdef DEBUG
        _logging = true;
#endif
    }
    
    return self;
}

- (BOOL)performFetch
{
    if ([self.delegate respondsToSelector:@selector(controllerWillPerformFetch:)]) {
        [self.delegate controllerWillPerformFetch:self];
    }
    
    if (self.fetchRequest) {
        
        if (self.cacheName) {
            [self createCacheWithRealm:[self cacheRealm]
                             cacheName:self.cacheName
                       forFetchRequest:self.fetchRequest
                    sectionNameKeyPath:self.sectionNameKeyPath];
        }
        else {
            [self createCacheWithRealm:[self cacheRealm]
                             cacheName:[self nameForFetchRequest:self.fetchRequest]
                       forFetchRequest:self.fetchRequest
                    sectionNameKeyPath:self.sectionNameKeyPath];
        }
        
        // Only register for changes after the cache was created!
        [self registerChangeNotifications];
        
        if ([self.delegate respondsToSelector:@selector(controllerDidPerformFetch:)]) {
            [self.delegate controllerDidPerformFetch:self];
        }
        
        return YES;
    }
    
    @throw [NSException exceptionWithName:@"RBQException"
                                   reason:@"Unable to perform fetch; fetchRequest must be set."
                                 userInfo:nil];
    
    return NO;
}

- (void)reset
{
    RLMRealm *cacheRealm = [self cacheRealm];
    
    [self unregisterChangeNotifications];
    
    [cacheRealm beginWriteTransaction];
    [cacheRealm deleteAllObjects];
    [cacheRealm commitWriteTransaction];
    
    [self performFetch];
}

- (id)objectAtIndexPath:(NSIndexPath *)indexPath
{
    RBQControllerCacheObject *cache = [self cache];
    
    if (indexPath.section < cache.sections.count) {
        RBQSectionCacheObject *section = cache.sections[indexPath.section];
        
        RLMRealm *realm = [RLMRealm realmWithConfiguration:self.fetchRequest.realmConfiguration error:nil];
        // Call refresh to guarantee latest results
        [realm refresh];
        
        RLMResults *sectionObjects = [self objectsForSection:section inRealm:realm];
        
        return [sectionObjects objectAtIndex:indexPath.row];
    }
    
    return nil;
}

- (NSIndexPath *)indexPathForObject:(RLMObjectBase *)object
{
    NSIndexPath *result = nil;
    RBQControllerCacheObject *cache = [self cache];
    NSString *sectionName = cache.sectionNameKeyPath.length > 0 ? [object valueForKeyPath:cache.sectionNameKeyPath] : @"";
    if (cache) {
        RBQSectionCacheObject *section = [cache.sections objectsWhere:@"name == %@",sectionName].firstObject;
        if (section)
        {
            RLMRealm *realm = [RLMRealm realmWithConfiguration:self.fetchRequest.realmConfiguration error:nil];
            RLMResults *sectionObjects = [self objectsForSection:section inRealm:realm];
            if (sectionObjects)
            {
                NSUInteger index = [sectionObjects indexOfObject:object];
                if (index != NSNotFound)
                {
                    result = [NSIndexPath indexPathForRow:index inSection:[cache.sections indexOfObject:section]];
                }
            }
        }
    }
    
    return result;
}

- (NSInteger)numberOfRowsForSectionIndex:(NSInteger)index
{
    RBQControllerCacheObject *cache = [self cache];
    
    if (index < cache.sections.count)
    {
        RBQSectionCacheObject *section = cache.sections[index];
        RLMRealm *realm = [RLMRealm realmWithConfiguration:self.fetchRequest.realmConfiguration error:nil];
        return [self objectsForSection:section inRealm:realm].count;
    }
    
    return 0;
}

- (NSInteger)numberOfSections
{
    RBQControllerCacheObject *cache = [self cache];
    
    if (cache) {
        return cache.sections.count;
    }
    
    return 0;
}

- (NSString *)titleForHeaderInSection:(NSInteger)section
{
    RBQControllerCacheObject *cache = [self cache];
    
    if (cache) {
        
        if (section < cache.sections.count) {
            RBQSectionCacheObject *sectionInfo = cache.sections[section];
            
            return sectionInfo.name;
        }
    }
    
    return @"";
}

- (NSUInteger)sectionIndexForSectionName:(NSString *)sectionName
{
    RBQControllerCacheObject *cache = [self cache];
    
    if (cache) {
        
        RLMResults *sectionWithName = [cache.sections objectsWhere:@"name == %@",sectionName];
        
        RBQSectionCacheObject *section = sectionWithName.firstObject;
        
        if (section) {
            return [cache.sections indexOfObject:section];
        }
    }
    
    return NSNotFound;
}

- (void)updateFetchRequest:(RBQFetchRequest *)fetchRequest
        sectionNameKeyPath:(NSString *)sectionNameKeyPath
           andPerformFetch:(BOOL)performFetch
{
    @synchronized(self) {
        // Turn off change notifications since we are replacing fetch request
        // Change notifications will be re-registered if performFetch is called
        [self unregisterChangeNotifications];
        
        // Updating the fetch request will force rebuild of cache automatically
        _sectionNameKeyPath = sectionNameKeyPath;
        _fetchRequest = fetchRequest;
        
        if (performFetch) {
            // Only performFetch if the change processing is finished
            [self performFetch];
        }
    }
}

#pragma mark - Getters

- (id<RLMCollection>)fetchedObjects
{
    if (self.fetchRequest) {
        return [self.fetchRequest fetchObjects];
    }
    
    return nil;
}

- (NSArray<NSString *> *)sectionIndexTitles
{
    RBQControllerCacheObject *cache = [self cache];
    
    if (cache) {
        NSArray *titles = [cache.sections valueForKey:@"name"];
        
        return titles;
    }
    
    return nil;
}

#pragma mark - Internal Cache

// Create the internal cache for a fetch request
- (void)createCacheWithRealm:(RLMRealm *)cacheRealm
                   cacheName:(NSString *)cacheName
             forFetchRequest:(RBQFetchRequest *)fetchRequest
          sectionNameKeyPath:(NSString *)sectionNameKeyPath
{
    id<RLMCollection> fetchResults = [fetchRequest fetchObjects];
    
    // Check if we have a cache already
    RBQControllerCacheObject *controllerCache = [RBQControllerCacheObject objectInRealm:cacheRealm
                                                                          forPrimaryKey:cacheName];
    
    [cacheRealm beginWriteTransaction];
    
    /**
     *  Reset the cache if the fetchRequest hash doesn't match
     *  The count in the cache is off from the fetch results
     *  The state was left in processing
     *  The section name key path has changed
     */
    if (controllerCache.fetchRequestHash != fetchRequest.hash ||
        controllerCache.objectsCount != fetchResults.count ||
        ![controllerCache.sectionNameKeyPath isEqualToString:sectionNameKeyPath] ||
        (fetchRequest.realm.configuration.inMemoryIdentifier.length == 0 && ![[NSFileManager defaultManager] fileExistsAtPath:fetchRequest.realm.configuration.fileURL.path]))
    {
        [cacheRealm deleteAllObjects];
        self.sectionObjectsPerThread = [NSMutableDictionary new];
        controllerCache = nil;
    }
    
    if (!controllerCache)
    {
        controllerCache = [RBQControllerCacheObject cacheWithName:cacheName
                                                 fetchRequestHash:fetchRequest.hash];
        controllerCache.objectsCount = fetchResults.count;
        controllerCache.fetchRequestHash = fetchRequest.hash;
        
        RBQSectionCacheObject *section = nil;
        
        // Iterate over the results to create the section information
        NSString *currentSectionTitle = nil;
        
        //No sections being used, so create default section
        if (!sectionNameKeyPath) {
            currentSectionTitle = @"";
            section = [self createSectionWithName:currentSectionTitle withKeyPath:sectionNameKeyPath fromFetchRequest:self.fetchRequest];
            [cacheRealm addOrUpdateObject:section];
            [controllerCache.sections addObject:section];
        } else {
            controllerCache.sectionNameKeyPath = sectionNameKeyPath;
            
            NSArray *originalDistinctBy = fetchRequest.distinctBy;
            NSMutableArray *newDistinctBy = [[NSMutableArray alloc] initWithArray:originalDistinctBy ?: @[]];
            [newDistinctBy addObject:sectionNameKeyPath];
            RBQFetchRequest *auxFR = [RBQFetchRequest fetchRequestWithEntityName:fetchRequest.entityName inRealm:fetchRequest.realm predicate:fetchRequest.predicate];
            auxFR.distinctBy = newDistinctBy;
            auxFR.sortDescriptors = [fetchRequest.sortDescriptors copy];
            
            id<RLMCollection> sectionObjects = [auxFR fetchObjects];
            for (RLMObjectBase *obj in sectionObjects)
            {
                NSString *sectionName = [obj valueForKeyPath:sectionNameKeyPath];
                section = [self createSectionWithName:sectionName withKeyPath:sectionNameKeyPath fromFetchRequest:self.fetchRequest];
                [cacheRealm addOrUpdateObject:section];
                [controllerCache.sections addObject:section];
            }
        }
        
        // Add cache to Realm
        [cacheRealm addOrUpdateObject:controllerCache];
    }
    
    [cacheRealm commitWriteTransaction];
}

- (RBQSectionCacheObject *)createSectionWithName:(NSString *)name withKeyPath:(NSString *)keyPath fromFetchRequest:(RBQFetchRequest *)fetchRequest
{
    RBQSectionCacheObject *section = [RBQSectionCacheObject cacheWithName:name];
    section.className = fetchRequest.entityName;
    NSPredicate *specificPredicate = keyPath.length > 0 ? [NSCompoundPredicate andPredicateWithSubpredicates:@[fetchRequest.predicate, [NSPredicate predicateWithFormat:@"%K = %@", keyPath, name]]] : fetchRequest.predicate;
    section.predicateData = [NSKeyedArchiver archivedDataWithRootObject:specificPredicate];
    section.distinctByData = [NSKeyedArchiver archivedDataWithRootObject:fetchRequest.distinctBy];
    section.sortDescriptorsData = [NSKeyedArchiver archivedDataWithRootObject:fetchRequest.sortDescriptors];
    return section;
}

#pragma mark - Notifications

// Register the change notification from RBQRealmNotificationManager
// Is no-op if the change notifications are already registered
- (void)registerChangeNotifications
{
    typeof(self) __weak weakSelf = self;
    
    // Setup run loop
    if (!self.notificationRunLoop) {
        dispatch_semaphore_t sem = dispatch_semaphore_create(0);
        dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0), ^{
            CFRunLoopPerformBlock(CFRunLoopGetCurrent(), kCFRunLoopDefaultMode, ^{
                weakSelf.notificationRunLoop = [NSRunLoop currentRunLoop];
                
                dispatch_semaphore_signal(sem);
            });
            
            CFRunLoopRun();
        });
        
        dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
    }
    
    CFRunLoopPerformBlock(self.notificationRunLoop.getCFRunLoop, kCFRunLoopDefaultMode, ^{
        if (weakSelf.notificationToken) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Warc-performSelector-leaks"
            if ([weakSelf.notificationToken respondsToSelector:NSSelectorFromString(@"invalidate")]) {
                [weakSelf.notificationToken performSelector:NSSelectorFromString(@"invalidate")];
            }else if ([weakSelf.notificationToken respondsToSelector:NSSelectorFromString(@"stop")]) {
                [weakSelf.notificationToken performSelector:NSSelectorFromString(@"stop")];
            }
#pragma clang diagnostic pop
            weakSelf.notificationToken = nil;
            weakSelf.notificationCollection = nil;
        }
        
        weakSelf.notificationCollection = weakSelf.fetchRequest.fetchObjects;
        weakSelf.notificationToken = [weakSelf.notificationCollection
                                      addNotificationBlock:^(id<RLMCollection>  _Nullable collection,
                                                             RLMCollectionChange * _Nullable change,
                                                             NSError * _Nullable error) {
                                          if (!error &&
                                              change) {
                                              BOOL useSem = NO;
                                              
                                              dispatch_semaphore_t sem = dispatch_semaphore_create(0);
                                              
                                              if (![NSThread isMainThread]) {
                                                  useSem = YES;
                                              }
                                              
                                              if ([self.delegate respondsToSelector:@selector(controllerWillChangeContent:)]) {
                                                  
                                                  [self runOnMainThread:^(){
                                                      [weakSelf.delegate controllerWillChangeContent:weakSelf];
                                                  }];
                                              }
                                              
                                              if (self.sectionNameKeyPath)
                                              {
                                                  /**
                                                   *  Refresh both the cache and main Realm.
                                                   *
                                                   *  NOTE: must use helper refresh method, so that
                                                   *  we prevent acting on the duplicate notification
                                                   *  triggered by the refresh.
                                                   *
                                                   *  This is a requirement for any refresh called
                                                   *  synchronously from a RLMRealmDidChangeNotification.
                                                   */
                                                  RLMRealm *cacheRealm = [self cacheRealm];
                                                  
                                                  RBQControllerCacheObject *cache = [self cacheInRealm:cacheRealm];
                                                  
#ifdef DEBUG
                                                  NSAssert(cache, @"Cache can't be nil!");
#endif
                                                  
                                                  [cacheRealm transactionWithBlock:^{
                                                      NSMutableArray *pKeys = [@[] mutableCopy];
                                                      NSString *pKeyField = nil;
                                                      for (NSNumber *index in change.insertions)
                                                      {
                                                          if ([index integerValue] >= collection.count) { continue; }
                                                          RLMObject *object = [collection objectAtIndex:[index integerValue]];
                                                          if (!pKeyField) { pKeyField = [[object class] primaryKey]; }
                                                          id pKey = [object valueForKey:pKeyField];
                                                          if (pKey) { [pKeys addObject:pKey]; }
                                                      }
                                                      for (NSNumber *index in change.modifications)
                                                      {
                                                          if ([index integerValue] >= collection.count) { continue; }
                                                          RLMObject *object = [collection objectAtIndex:[index integerValue]];
                                                          if (!pKeyField) { pKeyField = [[object class] primaryKey]; }
                                                          id pKey = [object valueForKey:pKeyField];
                                                          if (pKey) { [pKeys addObject:pKey]; }
                                                      }
                                                      
                                                      if (pKeys.count > 0)
                                                      {
                                                          RLMResults *objectsModified = [collection objectsWhere:@"%K IN %@", pKeyField, pKeys];
                                                          NSMutableSet *sectionNames = [NSMutableSet setWithArray:[objectsModified valueForKeyPath:self.sectionNameKeyPath]];
                                                          NSSet *oldNames = [NSSet setWithArray:[cache.sections valueForKey:@"name"]];
                                                          [sectionNames minusSet:oldNames];
                                                          for (NSString *sectionName in sectionNames)
                                                          {
                                                              RBQSectionCacheObject *section = [self createSectionWithName:sectionName withKeyPath:weakSelf.sectionNameKeyPath fromFetchRequest:weakSelf.fetchRequest];
                                                              [cacheRealm addOrUpdateObject:section];
                                                              [cache.sections addObject:section];
                                                          }
                                                      }
                                                      
                                                      NSMutableArray *sectionsToDelete = [@[] mutableCopy];
                                                      for (RBQSectionCacheObject *section in cache.sections)
                                                      {
                                                          if (section.isInvalidated) { continue; }
                                                          if ([collection objectsWhere:@"%K = %@", weakSelf.sectionNameKeyPath, section.name].count == 0)
                                                          {
                                                              [sectionsToDelete addObject:section];
                                                          }
                                                      }
                                                      [cacheRealm deleteObjects:sectionsToDelete];
                                                  }];
                                              }
                                              
                                              [self runOnMainThread:^(){
                                                  if ([weakSelf.delegate respondsToSelector:@selector(controllerDidChangeContent:)]) {
                                                      [weakSelf.delegate controllerDidChangeContent:weakSelf];
                                                  }
                                                  
                                                  if (useSem) {
                                                      dispatch_semaphore_signal(sem);
                                                  }
                                              }];
                                              
                                              if (useSem) {
                                                  dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
                                              }
                                          }
                                      }];
    });
    
    CFRunLoopWakeUp(self.notificationRunLoop.getCFRunLoop);
}

- (void)unregisterChangeNotifications
{
    // Remove the notifications
    if (self.notificationToken) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Warc-performSelector-leaks"
        if ([self.notificationToken respondsToSelector:NSSelectorFromString(@"invalidate")]) {
            [self.notificationToken performSelector:NSSelectorFromString(@"invalidate")];
        }else if ([self.notificationToken respondsToSelector:NSSelectorFromString(@"stop")]) {
            [self.notificationToken performSelector:NSSelectorFromString(@"stop")];
        }
#pragma clang diagnostic pop
        self.notificationToken = nil;
    }
    
    // Stop the run loop
    if (self.notificationRunLoop) {
        CFRunLoopStop(self.notificationRunLoop.getCFRunLoop);
        self.notificationRunLoop = nil;
    }
}

#pragma mark - Helpers

// Create instance of Realm for internal cache
- (RLMRealm *)cacheRealm
{
    if (self.cacheName) {
        
        if ([NSThread isMainThread] &&
            self.realmForMainThread) {
            
            return self.realmForMainThread;
        }
        
        RLMRealm *realm = [RBQFetchedResultsController realmForCacheName:self.cacheName];
        
        if ([NSThread isMainThread]) {
            
            self.realmForMainThread = realm;
        }
        
        return realm;
    }
    else {
        RLMRealmConfiguration *inMemoryConfiguration = [RLMRealmConfiguration defaultConfiguration];
        inMemoryConfiguration.inMemoryIdentifier = [self nameForFetchRequest:self.fetchRequest];
        inMemoryConfiguration.encryptionKey = nil;
        inMemoryConfiguration.objectClasses = @[RBQControllerCacheObject.class,
                                                RBQSectionCacheObject.class];
        
        RLMRealm *realm = [RLMRealm realmWithConfiguration:inMemoryConfiguration
                                                     error:nil];
        
        // Hold onto a strong reference so inMemory realm cache doesn't get deallocated
        // We don't use the cache since this is deprecated
        // If the realm path changed (new fetch request then hold onto the new one)
        if (!self.inMemoryRealm ||
            ![realm.configuration.fileURL.path.lastPathComponent isEqualToString:self.inMemoryRealm.configuration.fileURL.path.lastPathComponent]) {
            
            self.inMemoryRealm = realm;
        }
        
        return realm;
    }
    
    return nil;
}

// Retrieve internal cache
- (RBQControllerCacheObject *)cache
{
    RLMRealm *cacheRealm = [self cacheRealm];
    
    [cacheRealm refresh];
    
    RBQControllerCacheObject *cache = [self cacheInRealm:cacheRealm];
    
    return cache;
}

- (RBQControllerCacheObject *)cacheInRealm:(RLMRealm *)realm
{
    if (self.cacheName) {
        
        return [RBQControllerCacheObject objectInRealm:realm
                                         forPrimaryKey:self.cacheName];
    }
    else {
        return [RBQControllerCacheObject objectInRealm:realm
                                         forPrimaryKey:[self nameForFetchRequest:self.fetchRequest]];
    }
    
    return nil;
}

// Create a computed name for a fetch request
- (NSString *)nameForFetchRequest:(RBQFetchRequest *)fetchRequest
{
    return [NSString stringWithFormat:@"%lu-cache",(unsigned long)fetchRequest.hash];
}

/**
 Apparently iOS 7+ NSIndexPath's can sometimes be UIMutableIndexPaths:
 http://stackoverflow.com/questions/18919459/ios-7-beginupdates-endupdates-inconsistent/18920573#18920573
 
 This foils using them as dictionary keys since isEqual: fails between an equivalent NSIndexPath and
 UIMutableIndexPath.
 */
- (NSIndexPath *)keyForIndexPath:(NSIndexPath *)indexPath
{
    if ([indexPath class] == [NSIndexPath class]) {
        return indexPath;
    }
    return [NSIndexPath indexPathForRow:indexPath.row inSection:indexPath.section];
}

- (void)runOnMainThread:(void (^)(void))mainThreadBlock
{
    if ([NSThread isMainThread]) {
        mainThreadBlock();
    }
    else {
        dispatch_async(dispatch_get_main_queue(), mainThreadBlock);
    }
}

- (RLMResults *)objectsForSection:(RBQSectionCacheObject *)section inRealm:(RLMRealm *)realm
{
    RLMResults *result = nil;
    
    if (![[realm.schema.objectSchema valueForKey:@"className"] containsObject:section.className])
    {
        @throw [NSException exceptionWithName:@"RBQFetchedResultsController" reason:[NSString stringWithFormat:@"Class name -%@- cannot be found on realm's schema for Realm %@", section.className, realm] userInfo:@{@"realm" : realm}];
    } else {
        __uint64_t tid;
        pthread_threadid_np(NULL, &tid);
        NSString *threadID = [[NSString alloc] initWithFormat:@"%llu", tid];
        if (!self.sectionObjectsPerThread[threadID]) { self.sectionObjectsPerThread[threadID] = [NSMutableDictionary new]; }
        NSMutableDictionary *sectionObjects = self.sectionObjectsPerThread[threadID];
        if (!sectionObjects[section.name])
        {
            NSPredicate *predicate = section.predicateData ? [NSKeyedUnarchiver unarchiveObjectWithData:section.predicateData] : nil;
            RBQFetchRequest *fr = [RBQFetchRequest fetchRequestWithEntityName:section.className inRealm:realm predicate:predicate];
            fr.distinctBy = section.distinctByData ? [NSKeyedUnarchiver unarchiveObjectWithData:section.distinctByData] : nil;
            fr.sortDescriptors = section.sortDescriptorsData ? [NSKeyedUnarchiver unarchiveObjectWithData:section.sortDescriptorsData] : nil;
            sectionObjects[section.name] = [fr fetchObjects];
        }
        result = sectionObjects[section.name];
    }
    
    return result;
}

@end
