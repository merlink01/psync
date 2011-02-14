def prepare_chunk(path, chunk, data):
    #*** PreparedRanges(version.size, self.settings.attachmentSize)
    log = get_chunk_prep_log() or new_chunk_prep_log()
    log.add(chunk)

def commit_prepared_chunks(path, old_stat, new_stat, new_file_info):
    current_stat = stat_path(path)
    if current_stat == new_file_stat:
        log.info("File already committed: {}".format(path))
    elif current_stat != old_file_stat:
        raise Exception("File changed before committing.")
    # *** Deleted
    elif new_stat is Deleted:
        fs.move_to_trash(path)
        # ***
        fs.delete_empty_parents(path)
    else:
        # ***
        fs.move_to_trash(path)
        # ***
        write_chunks(path, new_file_info)
        # ***
        fs.touch(path, new_stat.mtime)

