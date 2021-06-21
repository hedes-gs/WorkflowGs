import axios, { AxiosInstance } from 'axios';
import { ServiceConfig } from './api.config'
import { ImageDto, toJsonImageDto } from '../model/DataModel';

export interface AlbumsService {
    addAlbum(Album: string, img: ImageDto): Promise<string>;
    deleteAlbum(Album: string, img: ImageDto): Promise<string>;
    getAlbumsLike(Album: string): Promise<string>;
    getAll(): Promise<string>;
}

export default class AlbumsServiceImpl implements AlbumsService {

    axiosInstance: AxiosInstance;
    baseUrlForAlbums: string;


    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
        this.baseUrlForAlbums = '/api/gs/albums/';
    }

    async getAlbumsLike(Album: string): Promise<string> {
        const urlToGetAlbumsLike = this.baseUrlForAlbums + 'getAlbumsLike/' + Album;
        return this.axiosInstance.get(urlToGetAlbumsLike)
            .then(resp => resp.data);
    }

    async addAlbum(Album: string, img: ImageDto): Promise<string> {
        const urlToAddAlbum = this.baseUrlForAlbums + 'addToImage/' + Album;

        return this.axiosInstance.post(urlToAddAlbum, toJsonImageDto(img), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }

    async deleteAlbum(Album: string, img: ImageDto): Promise<string> {
        const urlToAddAlbum = this.baseUrlForAlbums + 'deleteInImage/' + Album;

        return this.axiosInstance.post(urlToAddAlbum, toJsonImageDto(img), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }

    async getAll(): Promise<string> {
        const urlToGetRating = this.baseUrlForAlbums + 'all';
        return this.axiosInstance.get(urlToGetRating)
            .then(resp => resp.data);
    }


}
