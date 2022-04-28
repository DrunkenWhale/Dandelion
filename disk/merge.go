package disk

import (
	"Dandelion/util"
)

func KVArrayMerge(new []*util.KV, old []*util.KV) []*util.KV {

	newIndex := 0
	newBound := len(new)
	oldIndex := 0
	oldBound := len(old)

	res := make([]*util.KV, 0)

	for newIndex < newBound && oldIndex < oldBound {

		if new[newIndex].Key < old[oldIndex].Key {
			res = append(res, new[newIndex])
			newIndex++
		} else if new[newIndex].Key == old[oldIndex].Key {
			res = append(res, new[newIndex])
			newIndex++
			oldIndex++
		} else {
			res = append(res, old[oldIndex])
			oldIndex++
		}

	}

	for newIndex < newBound {
		res = append(res, new[newIndex])
		newIndex++
	}

	for oldIndex < oldBound {
		res = append(res, old[oldIndex])
		oldIndex++
	}

	return res
}
