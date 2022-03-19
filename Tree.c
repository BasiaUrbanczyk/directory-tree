#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "HashMap.h"
#include "err.h"

#include "Tree.h"
#include "path_utils.h"


//Synchronizing: each folder has its 'monitor', on which i use writers and readers scheme
//If i want to perform an operation on a folder, i firstly have to use a function called goingToWork, in which i try to visit every folder on the path to destination
//As i go down the path of folders, i mark on every folder that a reader has entered (obv only when its posible, otherwise i have to
//wait until it is possible), however, exceptionally on the last folder - the destination, i am counted as a writer
//When i reach the destination, then i can proceed to do the operation i wanted to
//After i finished the operation, i call the function returningFromWork, which marks on every folder i have visited, that the reader/writer
//has left


typedef struct Monitor Monitor;

struct Monitor {
    int readerNumber;
    int writerNumber;
    int writerWaiting;
    int readerWaiting;
    pthread_mutex_t mutex;
    pthread_cond_t toRead;
    pthread_cond_t toWrite;
    bool writerActive;
    int readerActive;
};

struct Tree {
    HashMap* subfolders;
    Monitor* monitor;
};


void readerStart(Monitor* m){
    pthread_mutex_lock(&m->mutex);
    while(m->writerNumber > 0 || m->writerWaiting > 0){
        m->readerWaiting++;
        pthread_cond_wait(&(m->toRead), &(m->mutex));
        m->readerWaiting--;
        if(m->readerActive > 0){
            m->readerActive--;
            break;
        }
    }
    m->readerNumber++;
    pthread_mutex_unlock(&m->mutex);
}

void readerEnd(Monitor* m){
    pthread_mutex_lock(&m->mutex);
    m->readerNumber--;
    if(m->readerNumber == 0 && m->readerActive == 0 && m->writerNumber == 0 && m->writerWaiting > 0 ){
        m->writerActive = true;
        pthread_cond_signal(&m->toWrite);
    }
    else if(m->readerNumber == 0 && m->writerNumber == 0){
        m->readerActive = m->readerWaiting;
        pthread_cond_broadcast(&m->toRead);
    }
    pthread_mutex_unlock(&m->mutex);
}

void writerStart(Monitor* m){
    pthread_mutex_lock(&m->mutex);
    while(m->writerNumber > 0 || m->writerWaiting > 0 || m->readerNumber > 0  || m->readerWaiting > 0){
        m->writerWaiting++;
        pthread_cond_wait(&m->toWrite, &m->mutex);
        m->writerWaiting--;
        if(m->writerActive){
            m->writerActive = false;
            break;
        }
    }
    m->writerNumber++;
    pthread_mutex_unlock(&m->mutex);
}

void writerEnd(Monitor* m){
    pthread_mutex_lock(&m->mutex);
    m->writerNumber--;
    if(m->readerWaiting > 0 && m->writerNumber == 0 && m->readerNumber == 0){
        m->readerActive = m->readerWaiting;
        pthread_cond_broadcast(&m->toRead);
    }
    else if(m->writerNumber == 0 && m->writerWaiting > 0 && m->readerNumber == 0){
        m->writerActive = true;
        pthread_cond_signal(&m->toWrite);
    }
    pthread_mutex_unlock(&m->mutex);
}


char* latestCommonAncestor(const char* path1, const char* path2){
    size_t len1 = strlen(path1);
    size_t len2 = strlen(path2);
    size_t len;
    if(len1 > len2){
        len = len2;
    }
    else{
        len = len1;
    }

    const char* start1 = path1 + 1;
    const char* start2 = path2 + 1;

    int commonLen = 1;
    while (start1 < path1 + len) {
        char* end1 = strchr(start1, '/');
        char* end2 = strchr(start2, '/');
        int lenTemp1 = 0;
        int lenTemp2 = 0;
        for (const char* p = start1; p != end1; ++p){
            lenTemp1++;
        }

        for (const char* p = start2; p != end2; ++p){
            lenTemp2++;
        }
        if(lenTemp1 != lenTemp2){
            break;
        }
        char folderName1[lenTemp1 + 1];
        strncpy(folderName1, start1, lenTemp1);
        folderName1[lenTemp1] = '\0';

        char folderName2[lenTemp2 + 1];
        strncpy(folderName2, start2, lenTemp2);
        folderName2[lenTemp2] = '\0';

        if(strcmp(folderName1, folderName2) != 0){
            break;
        }
        commonLen += lenTemp1 + 1;
        start1 = end1 + 1;
        start2 = end2 + 1;
    }

    char* path = malloc(sizeof(char)*(commonLen + 1));
    strncpy(path, path1, commonLen);
    path[commonLen] = '\0';

    return path;
}



void returningFromWork(Tree* foldersArray[], int lastPointersIndex, bool isSuccesful){
    if(isSuccesful){ //isSuccesful means, that the operation did not end with any error thus we managed to get to the final destination
        writerEnd(foldersArray[lastPointersIndex]->monitor);
        for(int i = lastPointersIndex - 1; i >= 0; i--){
            readerEnd(foldersArray[i]->monitor);
        }
        return;
    }

    for(int i = lastPointersIndex; i >= 0; i--){
        readerEnd(foldersArray[i]->monitor);
    }
}



//following the sequence of folders, eventually waiting on them, in order (hopefully) to reach the final destination
int goingToWork(Tree* tree, const char* path, Tree* foldersArray[], int* i){
    *i = 0;
    Tree* pointer = tree;
    pointer = hmap_get(pointer->subfolders, "/"); //moving to subfolders of "/" (first pointer at NULL)
    foldersArray[*i] = pointer;
    if(strlen(path) == 1){
        writerStart(pointer->monitor);
        return 0;
    }

    char folderTarget[MAX_FOLDER_NAME_LENGTH + 1];
    char* pathToParent = make_path_to_parent(path, folderTarget);
    size_t len = strlen(pathToParent);


    if(len == 1){ //path to parent like "/", we block the parent to make sure we can pursue any action on a child and the object is not going to disappear
        readerStart(pointer->monitor);
        pointer = hmap_get(pointer->subfolders, folderTarget);
        if(pointer == NULL){
            free(pathToParent);
            return ENOENT; //subfolder not present
        }
        writerStart(pointer->monitor);
        (*i)++;
        foldersArray[*i] = pointer;
        free(pathToParent);
        return 0;
    }


    readerStart(pointer->monitor);
    const char* name_start = pathToParent + 1; //element after "/"
    while (name_start < pathToParent + len) {
        char* name_end = strchr(name_start, '/'); // end of current path component, at '/'.
        int lenTemp = 0;
        for (const char* p = name_start; p != name_end; ++p){
            lenTemp++;
        }

        char folderName[lenTemp + 1];
        strncpy(folderName, name_start, lenTemp);
        folderName[lenTemp] = '\0';

        pointer = hmap_get(pointer->subfolders, folderName);
        if(pointer == NULL){
            free(pathToParent);
            return ENOENT;
        }
        (*i)++;
        foldersArray[*i] = pointer; //adding folder to array
        readerStart(pointer->monitor);
        name_start = name_end + 1;
    }


    Tree* folder = hmap_get(pointer->subfolders, folderTarget);
    if(folder == NULL){
        free(pathToParent);
        return ENOENT;
    }
    (*i)++;
    foldersArray[*i] = folder; //adding another folder to my array
    writerStart(folder->monitor);
    free(pathToParent);
    return 0;
}


//initializing all of the monitors components
void monitorInitialization(Tree* tree){
    tree->monitor = malloc(sizeof(Monitor));
    tree->monitor->readerNumber = 0;
    tree->monitor->writerNumber = 0;
    tree->monitor->writerWaiting = 0;
    tree->monitor->readerWaiting = 0;
    tree->monitor->readerActive = 0;
    tree->monitor->writerActive = false;
    if (pthread_mutex_init(&tree->monitor->mutex, 0) != 0){
        perror("Mutex failed\n");
        exit(1);
    }
    if(pthread_cond_init(&tree->monitor->toRead, 0) != 0){
        perror("Read condition variable failed\n");
        exit(1);
    }
    if(pthread_cond_init(&tree->monitor->toWrite, 0) != 0){
        perror("Write condition variable failed\n");
        exit(1);
    }
}


Tree* tree_new(){
    Tree* tree = (Tree*) malloc(sizeof(Tree));
    if (!tree){
        return NULL;
    }
    HashMap* map = hmap_new();
    tree->subfolders = map;

    monitorInitialization(tree);

    Tree* child = (Tree*) malloc(sizeof(Tree));

    child->subfolders = hmap_new();
    hmap_insert(map, "/", child);

    monitorInitialization(child);
    return tree;
}


void tree_free(Tree* tree){
    HashMap* map = tree->subfolders;
    if(hmap_size(map) == 0){
        pthread_mutex_destroy(&tree->monitor->mutex);
        pthread_cond_destroy(&tree->monitor->toRead);
        pthread_cond_destroy(&tree->monitor->toWrite);
        free(tree->monitor);
        hmap_free(map);
        free(tree);
        return;
    }
    const char* key;
    void* value;
    HashMapIterator it = hmap_iterator(map);
    while (hmap_next(map, &it, &key, &value)){
        tree_free(value);
        hmap_remove(map, key);
    }
    pthread_mutex_destroy(&tree->monitor->mutex);
    pthread_cond_destroy(&tree->monitor->toRead);
    pthread_cond_destroy(&tree->monitor->toWrite);
    free(tree->monitor);
    hmap_free(map);
    free(tree);
}


char* tree_list(Tree* tree, const char* path){
    size_t helpme = strlen(path);
    Tree* foldersArray[helpme]; //arrary with pointer to folders, in which we changed sth in their monitor
    int i;

    if(goingToWork(tree, path, foldersArray, &i) == ENOENT){
        returningFromWork(foldersArray, i, false);
        return NULL;
    }


    if(is_path_valid(path) == false){
        returningFromWork(foldersArray, i, true);
        return NULL;
    }

    size_t len = strlen(path);

    if(len == 1){ //path like "/", only direct subfolders
        Tree* pointer = tree;
        pointer = hmap_get(pointer->subfolders, "/"); //moving to subfolders of "/" (first pointer at NULL)
        HashMap* rootMap = pointer->subfolders; //map of roots subfolders
        char* stringMap = make_map_contents_string(rootMap);
        returningFromWork(foldersArray, i, true);
        return stringMap;
    }

    const char* name_start = path + 1; //element after "/"

    Tree* pointer = tree;
    pointer = hmap_get(pointer->subfolders, "/");
    while (name_start < path + len) {
        char* name_end = strchr(name_start, '/'); // End of current path component, at '/'.
        char temp[255];
        int lenTemp = 0;
        for (const char* p = name_start; p != name_end; ++p){
            temp[lenTemp] = *p;
            lenTemp++;
        }
        char folderName[lenTemp];
        memcpy(folderName, temp, lenTemp);
        pointer = hmap_get(pointer->subfolders, folderName);
        if(pointer == NULL){
            returningFromWork(foldersArray, i, true);
            return NULL;
        }
        name_start = name_end + 1;
    }

    HashMap* map = pointer->subfolders;
    char* res = make_map_contents_string(map);
    returningFromWork(foldersArray, i, true);
    return res;
}


int tree_create(Tree* tree, const char* path){
    size_t helpme = strlen(path);
    Tree* foldersArray[helpme];
    int i;

    if(strlen(path) == 1){ //path = "/"
        return EEXIST;
    }

    char ehhh[MAX_FOLDER_NAME_LENGTH + 1];
    char* pathToParent = make_path_to_parent(path, ehhh);
    if(goingToWork(tree, pathToParent, foldersArray, &i) == ENOENT){
        free(pathToParent);
        returningFromWork(foldersArray, i, false);
        return ENOENT;
    }

    free(pathToParent);
    if(is_path_valid(path) == false){
        returningFromWork(foldersArray, i, true);
        return EINVAL;
    }

    char component[MAX_FOLDER_NAME_LENGTH + 1];
    char* newPath = make_path_to_parent(path, component);
    size_t len = strlen(newPath);
    if(len == 1){ //path like "/"
        Tree* pointer = tree;
        pointer = hmap_get(pointer->subfolders, "/");
        if(hmap_get(pointer->subfolders, component) != NULL){
            free(newPath);
            returningFromWork(foldersArray, i, true);
            return EEXIST;
        }
        Tree* newFolder = (Tree*) malloc(sizeof(Tree));
        char temp[MAX_FOLDER_NAME_LENGTH + 1];
        strncpy(temp, component, MAX_FOLDER_NAME_LENGTH + 1);
        newFolder->subfolders = hmap_new();
        hmap_insert(pointer->subfolders, component, newFolder);
        monitorInitialization(newFolder);
        free(newPath);
        returningFromWork(foldersArray, i, true);
        return 0;
    }

    const char* name_start = newPath + 1; //element after "/"
    Tree* pointer = tree;
    pointer = hmap_get(pointer->subfolders, "/");

    while (name_start < newPath + len) {
        char* name_end = strchr(name_start, '/'); // End of current path component, at '/'.
        int lenTemp = 0; //name length of the current folder
        for (const char* p = name_start; p != name_end; ++p){
            lenTemp++;
        }

        char folderName[lenTemp + 1];
        strncpy(folderName, name_start, lenTemp);
        folderName[lenTemp] = '\0';

        pointer = hmap_get(pointer->subfolders, folderName);
        if(pointer == NULL){
            free(newPath);
            returningFromWork(foldersArray, i, true);
            return ENOENT;
        }
        name_start = name_end + 1;
    }


    HashMap* map = pointer->subfolders;
    if(hmap_get(map, component) != NULL){
        free(newPath);
        returningFromWork(foldersArray, i, true);
        return EEXIST;
    }

    Tree* node = (Tree*) malloc(sizeof(Tree));
    char temp2[MAX_FOLDER_NAME_LENGTH + 1]; //memory allocation for new name
    strncpy(temp2, component, MAX_FOLDER_NAME_LENGTH + 1);
    node->subfolders = hmap_new();
    hmap_insert(map, component, node);
    monitorInitialization(node);
    free(newPath);
    returningFromWork(foldersArray, i, true);
    return 0;
}



int tree_remove(Tree* tree, const char* path){
    size_t helpme = strlen(path);
    Tree* foldersArray[helpme];
    int i;

    size_t lenCheck = strlen(path);
    if(lenCheck == 1){ //root given
        return EBUSY;
    }

    char ehhh[MAX_FOLDER_NAME_LENGTH + 1];
    char* pathToParent = make_path_to_parent(path, ehhh);
    if(goingToWork(tree, pathToParent, foldersArray, &i) == ENOENT){
        free(pathToParent);
        returningFromWork(foldersArray, i, false);
        return ENOENT;
    }

    free(pathToParent);

    if(is_path_valid(path) == false){
        returningFromWork(foldersArray, i, true);
        return EINVAL;
    }

    char component[MAX_FOLDER_NAME_LENGTH + 1];
    char* newPath = make_path_to_parent(path, component);
    size_t len = strlen(newPath);


    if(len == 1){ //path to parent like "/"
        Tree* pointer = tree;
        pointer = hmap_get(pointer->subfolders, "/");
        Tree* folderToRemove
        = hmap_get(pointer->subfolders, component);
        if(folderToRemove == NULL){
            free(newPath);
            returningFromWork(foldersArray, i, true);
            return ENOENT;
        }
        HashMap* foldersMap = folderToRemove->subfolders;
        if(hmap_size(foldersMap) != 0){
            free(newPath);
            returningFromWork(foldersArray, i, true);
            return ENOTEMPTY;
        }

        hmap_free(folderToRemove->subfolders);
        pthread_mutex_destroy(&folderToRemove->monitor->mutex);
        pthread_cond_destroy(&folderToRemove->monitor->toRead);
        pthread_cond_destroy(&folderToRemove->monitor->toWrite);
        free(folderToRemove->monitor);
        free(folderToRemove);
        hmap_remove(pointer->subfolders, component);
        free(newPath);
        returningFromWork(foldersArray, i, true);
        return 0;
    }


    const char* name_start = newPath + 1; //element after "/"
    Tree* pointer = tree;
    pointer = hmap_get(pointer->subfolders, "/");
    while (name_start < newPath + len) {
        char* name_end = strchr(name_start, '/'); // end of current path component, at '/'.
        int lenTemp = 0;
        for (const char* p = name_start; p != name_end; ++p){
            lenTemp++;
        }

        char folderName[lenTemp + 1];
        strncpy(folderName, name_start, lenTemp);
        folderName[lenTemp] = '\0';

        pointer = hmap_get(pointer->subfolders, folderName);
        if(pointer == NULL){
            free(newPath);
            returningFromWork(foldersArray, i, true);
            return ENOENT;
        }
        name_start = name_end + 1;
    }

    Tree* folderToRemove = hmap_get(pointer->subfolders, component);
    if(folderToRemove == NULL){
        free(newPath);
        returningFromWork(foldersArray, i, true);
        return ENOENT;
    }
    HashMap* foldersMap = folderToRemove->subfolders;
    if(hmap_size(foldersMap) != 0){ //folder to delete not empty
        free(newPath);
        returningFromWork(foldersArray, i, true);
        return ENOTEMPTY;
    }

    //free the deleted folder
    hmap_free(folderToRemove->subfolders);
    pthread_mutex_destroy(&folderToRemove->monitor->mutex);
    pthread_cond_destroy(&folderToRemove->monitor->toRead);
    pthread_cond_destroy(&folderToRemove->monitor->toWrite);
    free(folderToRemove->monitor);
    free(folderToRemove);
    hmap_remove(pointer->subfolders, component);
    free(newPath);
    returningFromWork(foldersArray, i, true);
    return 0;
}



bool is_substring(const char* first, const char* second){ //first is the source, second - the destination
    size_t lenFirst = strlen(first);
    size_t lenSecond = strlen(second);

    if(lenFirst > lenSecond){
        return false;
    }

    char string[lenFirst + 1];
    strncpy(string, second, lenFirst);
    string[lenFirst] = '\0';
    if(strcmp(first, string) == 0){
        return true;
    }
    return false;
}



int tree_move(Tree* tree, const char* source, const char* target){

    size_t lenCheck = strlen(source);
    if(lenCheck == 1){ //root given
        return EBUSY;
    }

    size_t lenCheckTarget = strlen(target);
    if(lenCheckTarget == 1){
        return EEXIST;
    }

    char* toLatestAncestor = latestCommonAncestor(source, target);
    size_t helpme = strlen(toLatestAncestor);
    Tree* tablicaKolejnychFolderow[helpme];
    int i;
    if(goingToWork(tree, toLatestAncestor, tablicaKolejnychFolderow, &i) == ENOENT){
        free(toLatestAncestor);
        returningFromWork(tablicaKolejnychFolderow, i, false);
        return ENOENT;
    }
    free(toLatestAncestor);

    if(is_path_valid(source) == false || is_path_valid(target) == false){
        returningFromWork(tablicaKolejnychFolderow, i, true);
        return EINVAL;
    }


    char component[MAX_FOLDER_NAME_LENGTH + 1];
    char* pathSourceParent = make_path_to_parent(source, component);
    size_t len = strlen(pathSourceParent);

    Tree* sourceParent = tree;
    sourceParent = hmap_get(sourceParent->subfolders, "/");
    Tree* folderToMove;

    if(len == 1){ //path to parent like "/"
        folderToMove = hmap_get(sourceParent->subfolders, component);
        if(folderToMove == NULL){
            free(pathSourceParent);
            returningFromWork(tablicaKolejnychFolderow, i, true);
            return ENOENT;
        }

    }
    else{
        const char* name_start = pathSourceParent + 1; //element after "/"
        while (name_start < pathSourceParent + len) {
            char* name_end = strchr(name_start, '/'); // End of current path component, at '/'.
            int lenTemp = 0;
            for (const char* p = name_start; p != name_end; ++p){
                lenTemp++;
            }

            char folderName[lenTemp + 1];
            strncpy(folderName, name_start, lenTemp);
            folderName[lenTemp] = '\0';

            sourceParent = hmap_get(sourceParent->subfolders, folderName);
            if(sourceParent == NULL){
                free(pathSourceParent);
                returningFromWork(tablicaKolejnychFolderow, i, true);
                return ENOENT;
            }
            name_start = name_end + 1;
        }
        folderToMove = hmap_get(sourceParent->subfolders, component);
        if(folderToMove == NULL){
            free(pathSourceParent);
            returningFromWork(tablicaKolejnychFolderow, i, true);
            return ENOENT;
        }
    }


    char componentTarget[MAX_FOLDER_NAME_LENGTH + 1];
    char* pathSourceParentTarget = make_path_to_parent(target, componentTarget);
    size_t lenTarget = strlen(pathSourceParentTarget);

    Tree* targetParent = tree;
    targetParent = hmap_get(targetParent->subfolders, "/");

    if(lenTarget == 1){ //path to parent like "/"
        if(hmap_get(targetParent->subfolders, componentTarget) != NULL){
            free(pathSourceParent);
            free(pathSourceParentTarget);
            returningFromWork(tablicaKolejnychFolderow, i, true);
            return EEXIST;
        }
        hmap_remove(sourceParent->subfolders, component);
        hmap_insert(targetParent->subfolders, componentTarget, folderToMove);
        char temp[MAX_FOLDER_NAME_LENGTH + 1];
        strncpy(temp, componentTarget, MAX_FOLDER_NAME_LENGTH + 1);
    }
    else{
        const char* name_start = pathSourceParentTarget + 1; //element after "/"
        while (name_start < pathSourceParentTarget + lenTarget) {
            char* name_end = strchr(name_start, '/'); // End of current path componentTarget, at '/'.
            int lenTemp = 0; //name lenght of the current folder
            for (const char* p = name_start; p != name_end; ++p){
                lenTemp++;
            }

            char folderName[lenTemp + 1];
            strncpy(folderName, name_start, lenTemp);
            folderName[lenTemp] = '\0';

            targetParent = hmap_get(targetParent->subfolders, folderName);
            if(targetParent == NULL){
                free(pathSourceParent);
                free(pathSourceParentTarget);
                returningFromWork(tablicaKolejnychFolderow, i, true);
                return ENOENT;
            }
            name_start = name_end + 1;
        }

        if(hmap_get(targetParent->subfolders, componentTarget) != NULL){
            free(pathSourceParent);
            free(pathSourceParentTarget);
            returningFromWork(tablicaKolejnychFolderow, i, true);
            return EEXIST;
        }

        if(is_substring(source, target) == true){
            free(pathSourceParent);
            free(pathSourceParentTarget);
            returningFromWork(tablicaKolejnychFolderow, i, true);
            return -1; //source is subfolder of the target
        }

        hmap_remove(sourceParent->subfolders, component);
        hmap_insert(targetParent->subfolders, componentTarget, folderToMove);
        char temp2[MAX_FOLDER_NAME_LENGTH + 1]; //memory for new name of the folder
        strncpy(temp2, componentTarget, MAX_FOLDER_NAME_LENGTH + 1);
    }

    free(pathSourceParent);
    free(pathSourceParentTarget);
    returningFromWork(tablicaKolejnychFolderow, i, true);
    return 0;
}


